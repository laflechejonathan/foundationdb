/*
 * S3BlobStore.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/S3BlobStore.h"

#include "flow/IConnection.h"
#include "md5/md5.h"
#include "libb64/encode.h"
#include "fdbclient/sha1/SHA1.h"
#include <climits>
#include <time.h>
#include <iomanip>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include "flow/Hostname.h"
#include "flow/UnitTest.h"
#include "rapidxml/rapidxml.hpp"
#ifdef WITH_AWS_BACKUP
#include "fdbclient/FDBAWSCredentialsProvider.h"
#endif

#include "flow/actorcompiler.h" // has to be last include

using namespace rapidxml;

std::string guessRegionFromDomain(std::string domain) {
	static const std::vector<const char*> knownServices = { "s3.", "cos.", "oss-", "obs." };
	boost::algorithm::to_lower(domain);

	for (int i = 0; i < knownServices.size(); ++i) {
		const char* service = knownServices[i];

		std::size_t p = domain.find(service);
		if (p == std::string::npos || (p >= 1 && domain[p - 1] != '.')) {
			// eg. 127.0.0.1, example.com, s3-service.example.com, mys3.example.com
			continue;
		}

		StringRef h = StringRef(domain).substr(p);

		if (!h.startsWith("oss-"_sr)) {
			h.eat(service); // ignore s3 service
		}

		return h.eat(".").toString();
	}

	return "";
}

static Optional<S3BlobStoreEndpoint::Credentials> parseCredentials(Optional<StringRef> const& credString) {
	if (credString.present()) {
		StringRef c = credString.get();
		StringRef key = c.eat(":");
		StringRef secret = c.eat(":");
		StringRef securityToken = c.eat();
		return S3BlobStoreEndpoint::Credentials{ key.toString(), secret.toString(), securityToken.toString() };
	}
	return Optional<S3BlobStoreEndpoint::Credentials>();
}

S3BlobStoreEndpoint::S3BlobStoreEndpoint(std::string const& host,
                                         std::string const& service,
                                         std::string region,
                                         Optional<std::string> const& proxyHost,
                                         Optional<std::string> const& proxyPort,
                                         Optional<Credentials> const& creds,
                                         BlobKnobs const& knobs,
                                         HTTP::Headers extraHeaders)
  : IBlobStoreEndpoint(host,
                       service,
                       region.empty() ? guessRegionFromDomain(host) : region,
                       proxyHost,
                       proxyPort,
                       knobs,
                       extraHeaders),
    credentials(creds), lookupKey(this->credentials.present() && this->credentials.get().key.empty()),
    lookupSecret(this->credentials.present() && this->credentials.get().secret.empty()) {
	if (getRegion().empty() && CLIENT_KNOBS->HTTP_REQUEST_AWS_V4_HEADER) {
		throw std::string(
		    "Failed to get region from host or parameter in url, region is required for aws v4 signature");
	}
}

S3BlobStoreEndpoint::S3BlobStoreEndpoint(std::string const& host,
                                         std::string const& service,
                                         std::string region,
                                         Optional<std::string> const& proxyHost,
                                         Optional<std::string> const& proxyPort,
                                         Optional<StringRef> const& credString,
                                         BlobKnobs const& knobs,
                                         HTTP::Headers extraHeaders)
  : S3BlobStoreEndpoint(host,
                        service,
                        region,
                        proxyHost,
                        proxyPort,
                        parseCredentials(credString),
                        knobs,
                        extraHeaders) {}

std::string S3BlobStoreEndpoint::getResourceURL(std::string resource, std::string params) const {
	std::string hostPort = host;
	if (!service.empty()) {
		hostPort.append(":");
		hostPort.append(service);
	}

	// If secret isn't being looked up from credentials files then it was passed explicitly in the URL so show it here.
	std::string credsString;
	if (credentials.present()) {
		if (!lookupKey) {
			credsString = credentials.get().key;
		}
		if (!lookupSecret) {
			credsString += ":" + credentials.get().secret;
		}
		if (!lookupSecret) {
			credsString +=
			    credentials.get().securityToken.empty()
			        ? std::string(":") + credentials.get().secret
			        : std::string(":") + credentials.get().secret + std::string(":") + credentials.get().securityToken;
		}
		credsString += "@";
	}

	std::string r = format("blobstore://%s%s/%s", credsString.c_str(), hostPort.c_str(), resource.c_str());

	// Get params that are deviations from knob defaults
	std::string knobParams = knobs.getURLParameters();
	if (!knobParams.empty()) {
		if (!params.empty()) {
			params.append("&");
		}
		params.append(knobParams);
	}

	for (const auto& [k, v] : extraHeaders) {
		if (!params.empty()) {
			params.append("&");
		}
		params.append("header=");
		params.append(k);
		params.append(":");
		params.append(v);
	}

	if (!params.empty())
		r.append("?").append(params);

	return r;
}

std::string constructResourcePath(Reference<S3BlobStoreEndpoint> b,
                                  const std::string& bucket,
                                  const std::string& object) {
	std::string resource;

	if (b->getHost().find(bucket + ".") != 0) {
		resource += std::string("/") + bucket; // not virtual hosting mode
	}

	if (!object.empty()) {
		resource += "/";
		resource += object;
	}

	return resource;
}

ACTOR Future<bool> bucketExists_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket) {
	wait(b->requestRateRead->getAllowance(1));

	std::string resource = constructResourcePath(b, bucket, "");
	HTTP::Headers headers;

	Reference<HTTP::IncomingResponse> r = wait(b->doRequest("HEAD", resource, headers, nullptr, 0, { 200, 404 }));
	return r->code == 200;
}

Future<bool> S3BlobStoreEndpoint::bucketExists(std::string const& bucket) {
	return bucketExists_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<bool> objectExists_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket, std::string object) {
	wait(b->requestRateRead->getAllowance(1));

	std::string resource = constructResourcePath(b, bucket, object);
	HTTP::Headers headers;

	Reference<HTTP::IncomingResponse> r = wait(b->doRequest("HEAD", resource, headers, nullptr, 0, { 200, 404 }));
	return r->code == 200;
}

Future<bool> S3BlobStoreEndpoint::objectExists(std::string const& bucket, std::string const& object) {
	return objectExists_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteObject_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket, std::string object) {
	wait(b->requestRateDelete->getAllowance(1));

	std::string resource = constructResourcePath(b, bucket, object);
	HTTP::Headers headers;
	// 200 or 204 means object successfully deleted, 404 means it already doesn't exist, so any of those are considered
	// successful
	Reference<HTTP::IncomingResponse> r =
	    wait(b->doRequest("DELETE", resource, headers, nullptr, 0, { 200, 204, 404 }));

	// But if the object already did not exist then the 'delete' is assumed to be successful but a warning is logged.
	if (r->code == 404) {
		TraceEvent(SevWarnAlways, "S3BlobStoreEndpointDeleteObjectMissing")
		    .detail("Host", b->host)
		    .detail("Bucket", bucket)
		    .detail("Object", object);
	}

	return Void();
}

Future<Void> S3BlobStoreEndpoint::deleteObject(std::string const& bucket, std::string const& object) {
	return deleteObject_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteRecursively_impl(Reference<S3BlobStoreEndpoint> b,
                                          std::string bucket,
                                          std::string prefix,
                                          int* pNumDeleted,
                                          int64_t* pBytesDeleted) {
	state PromiseStream<S3BlobStoreEndpoint::ListResult> resultStream;
	// Start a recursive parallel listing which will send results to resultStream as they are received
	state Future<Void> done = b->listObjectsStream(bucket, resultStream, prefix, '/', std::numeric_limits<int>::max());
	// Wrap done in an actor which will send end_of_stream since listObjectsStream() does not (so that many calls can
	// write to the same stream)
	done = map(done, [=](Void) mutable {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	state std::list<Future<Void>> deleteFutures;
	try {
		loop {
			choose {
				// Throw if done throws, otherwise don't stop until end_of_stream
				when(wait(done)) {
					done = Never();
				}

				when(S3BlobStoreEndpoint::ListResult list = waitNext(resultStream.getFuture())) {
					for (auto& object : list.objects) {
						deleteFutures.push_back(map(b->deleteObject(bucket, object.name), [=](Void) -> Void {
							if (pNumDeleted != nullptr) {
								++*pNumDeleted;
							}
							if (pBytesDeleted != nullptr) {
								*pBytesDeleted += object.size;
							}
							return Void();
						}));
					}
				}
			}

			// This is just a precaution to avoid having too many outstanding delete actors waiting to run
			while (deleteFutures.size() > CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS) {
				wait(deleteFutures.front());
				deleteFutures.pop_front();
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}

	while (deleteFutures.size() > 0) {
		wait(deleteFutures.front());
		deleteFutures.pop_front();
	}

	return Void();
}

Future<Void> S3BlobStoreEndpoint::deleteRecursively(std::string const& bucket,
                                                    std::string prefix,
                                                    int* pNumDeleted,
                                                    int64_t* pBytesDeleted) {
	return deleteRecursively_impl(
	    Reference<S3BlobStoreEndpoint>::addRef(this), bucket, prefix, pNumDeleted, pBytesDeleted);
}

ACTOR Future<Void> createBucket_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket) {
	wait(b->requestRateWrite->getAllowance(1));

	bool exists = wait(b->bucketExists(bucket));
	if (!exists) {
		std::string resource = constructResourcePath(b, bucket, "");
		HTTP::Headers headers;

		std::string region = b->getRegion();
		if (region.empty()) {
			Reference<HTTP::IncomingResponse> r =
			    wait(b->doRequest("PUT", resource, headers, nullptr, 0, { 200, 409 }));
		} else {
			UnsentPacketQueue packets;
			StringRef body(format("<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
			                      "  <LocationConstraint>%s</LocationConstraint>"
			                      "</CreateBucketConfiguration>",
			                      region.c_str()));
			PacketWriter pw(packets.getWriteBuffer(), nullptr, Unversioned());
			pw.serializeBytes(body);

			Reference<HTTP::IncomingResponse> r =
			    wait(b->doRequest("PUT", resource, headers, &packets, body.size(), { 200, 409 }));
		}
	}
	return Void();
}

Future<Void> S3BlobStoreEndpoint::createBucket(std::string const& bucket) {
	return createBucket_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<int64_t> objectSize_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket, std::string object) {
	wait(b->requestRateRead->getAllowance(1));

	std::string resource = constructResourcePath(b, bucket, object);
	HTTP::Headers headers;

	Reference<HTTP::IncomingResponse> r = wait(b->doRequest("HEAD", resource, headers, nullptr, 0, { 200, 404 }));
	if (r->code == 404)
		throw file_not_found();
	return r->data.contentLen;
}

Future<int64_t> S3BlobStoreEndpoint::objectSize(std::string const& bucket, std::string const& object) {
	return objectSize_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

// If the credentials expire, the connection will eventually fail and be discarded from the pool, and then a new
// connection will be constructed, which will call this again to get updated credentials
static S3BlobStoreEndpoint::Credentials getSecretSdk() {
#ifdef WITH_AWS_BACKUP
	double elapsed = -timer_monotonic();
	Aws::Auth::AWSCredentials awsCreds = FDBAWSCredentialsProvider::getAwsCredentials();
	elapsed += timer_monotonic();

	if (awsCreds.IsEmpty()) {
		TraceEvent(SevWarn, "S3BlobStoreAWSCredsEmpty");
		throw backup_auth_missing();
	}

	S3BlobStoreEndpoint::Credentials fdbCreds;
	fdbCreds.key = awsCreds.GetAWSAccessKeyId();
	fdbCreds.secret = awsCreds.GetAWSSecretKey();
	fdbCreds.securityToken = awsCreds.GetSessionToken();

	TraceEvent("S3BlobStoreGotSdkCredentials").suppressFor(60).detail("Duration", elapsed);

	return fdbCreds;
#else
	TraceEvent(SevError, "S3BlobStoreNoSDK");
	throw backup_auth_missing();
#endif
}

ACTOR Future<Void> updateSecret_impl(Reference<S3BlobStoreEndpoint> b) {
	if (!(b->lookupKey || b->lookupSecret || b->knobs.sdk_auth)) {
		return Void();
	}

	if (b->knobs.sdk_auth) {
		b->credentials = getSecretSdk();
		return Void();
	}
	std::vector<std::string>* pFiles = (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
	if (pFiles == nullptr)
		return Void();

	if (!b->credentials.present()) {
		return Void();
	}

	state std::vector<Future<Optional<json_spirit::mObject>>> reads;
	for (auto& f : *pFiles)
		reads.push_back(tryReadJSONFile(f));

	wait(waitForAll(reads));

	std::string accessKey = b->lookupKey ? "" : b->credentials.get().key;
	std::string credentialsFileKey = accessKey + "@" + b->host;

	int invalid = 0;

	for (auto& f : reads) {
		// If value not present then the credentials file wasn't readable or valid.  Continue to check other results.
		if (!f.get().present()) {
			++invalid;
			continue;
		}

		JSONDoc doc(f.get().get());
		if (doc.has("accounts") && doc.last().type() == json_spirit::obj_type) {
			JSONDoc accounts(doc.last().get_obj());
			if (accounts.has(credentialsFileKey, false) && accounts.last().type() == json_spirit::obj_type) {
				JSONDoc account(accounts.last());
				S3BlobStoreEndpoint::Credentials creds = b->credentials.get();
				if (b->lookupKey) {
					std::string apiKey;
					if (account.tryGet("api_key", apiKey))
						creds.key = apiKey;
					else
						continue;
				}
				if (b->lookupSecret) {
					std::string secret;
					if (account.tryGet("secret", secret))
						creds.secret = secret;
					else
						continue;
				}
				std::string token;
				if (account.tryGet("token", token))
					creds.securityToken = token;
				b->credentials = creds;
				return Void();
			}
		}
	}

	// If any sources were invalid
	if (invalid > 0)
		throw backup_auth_unreadable();

	// All sources were valid but didn't contain the desired info
	throw backup_auth_missing();
}

Future<Void> S3BlobStoreEndpoint::updateSecret() {
	return updateSecret_impl(Reference<S3BlobStoreEndpoint>::addRef(this));
}

void S3BlobStoreEndpoint::setAllAuthHeaders(const std::string& verb,
                                            const std::string& resource,
                                            HTTP::Headers& headers,
                                            std::string date,
                                            std::string datestamp) {
	// Finish/update the request headers (which includes Date header)
	// This must be done AFTER the connection is ready because if credentials are coming from disk they are
	// refreshed when a new connection is established and setAuthHeaders() would need the updated secret.
	if (credentials.present() && !credentials.get().securityToken.empty())
		headers["x-amz-security-token"] = credentials.get().securityToken;
	if (CLIENT_KNOBS->HTTP_REQUEST_AWS_V4_HEADER) {
		setV4AuthHeaders(verb, resource, headers);
	} else {
		setAuthHeaders(verb, resource, headers);
	}
}

std::string S3BlobStoreEndpoint::canonicalizeURI(const std::string& resource,
                                                 std::vector<std::string>& queryParameters) {
	bool isV4 = CLIENT_KNOBS->HTTP_REQUEST_AWS_V4_HEADER;
	StringRef resourceRef(resource);
	resourceRef.eat("/");
	std::string canonicalURI("/" + resourceRef.toString());
	size_t q = canonicalURI.find_last_of('?');
	if (q != canonicalURI.npos)
		canonicalURI.resize(q);
	if (isV4) {
		canonicalURI = HTTP::awsV4URIEncode(canonicalURI, false);
	} else {
		canonicalURI = HTTP::urlEncode(canonicalURI);
	}

	// Create the canonical query string
	std::string queryString;
	q = resource.find_last_of('?');
	if (q != queryString.npos)
		queryString = resource.substr(q + 1);

	StringRef qStr(queryString);
	StringRef queryParameter;
	while ((queryParameter = qStr.eat("&")) != StringRef()) {
		StringRef param = queryParameter.eat("=");
		StringRef value = queryParameter.eat();

		if (isV4) {
			queryParameters.push_back(HTTP::awsV4URIEncode(param.toString(), true) + "=" +
			                          HTTP::awsV4URIEncode(value.toString(), true));
		} else {
			queryParameters.push_back(HTTP::urlEncode(param.toString()) + "=" + HTTP::urlEncode(value.toString()));
		}
	}

	return canonicalURI;
}

ACTOR Future<Void> listObjectsStream_impl(Reference<S3BlobStoreEndpoint> bstore,
                                          std::string bucket,
                                          PromiseStream<S3BlobStoreEndpoint::ListResult> results,
                                          Optional<std::string> prefix,
                                          Optional<char> delimiter,
                                          int maxDepth,
                                          std::function<bool(std::string const&)> recurseFilter) {
	// Request 1000 keys at a time, the maximum allowed
	state std::string resource = constructResourcePath(bstore, bucket, "");

	resource.append("/?max-keys=1000");
	if (prefix.present())
		resource.append("&prefix=").append(prefix.get());
	if (delimiter.present())
		resource.append("&delimiter=").append(std::string(1, delimiter.get()));
	resource.append("&marker=");
	state std::string lastFile;
	state bool more = true;

	state std::vector<Future<Void>> subLists;

	while (more) {
		wait(bstore->concurrentLists.take());
		state FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		HTTP::Headers headers;
		state std::string fullResource = resource + lastFile;
		lastFile.clear();
		Reference<HTTP::IncomingResponse> r =
		    wait(bstore->doRequest("GET", fullResource, headers, nullptr, 0, { 200 }));
		listReleaser.release();

		try {
			S3BlobStoreEndpoint::ListResult listResult;
			xml_document<> doc;

			// Copy content because rapidxml will modify it during parse
			std::string content = r->data.content;
			doc.parse<0>((char*)content.c_str());

			// There should be exactly one node
			xml_node<>* result = doc.first_node();
			if (result == nullptr || strcmp(result->name(), "ListBucketResult") != 0) {
				throw http_bad_response();
			}

			xml_node<>* n = result->first_node();
			while (n != nullptr) {
				const char* name = n->name();
				if (strcmp(name, "IsTruncated") == 0) {
					const char* val = n->value();
					if (strcmp(val, "true") == 0) {
						more = true;
					} else if (strcmp(val, "false") == 0) {
						more = false;
					} else {
						throw http_bad_response();
					}
				} else if (strcmp(name, "Contents") == 0) {
					S3BlobStoreEndpoint::ObjectInfo object;

					xml_node<>* key = n->first_node("Key");
					if (key == nullptr) {
						throw http_bad_response();
					}
					object.name = key->value();

					xml_node<>* size = n->first_node("Size");
					if (size == nullptr) {
						throw http_bad_response();
					}
					object.size = strtoull(size->value(), nullptr, 10);

					listResult.objects.push_back(object);
				} else if (strcmp(name, "CommonPrefixes") == 0) {
					xml_node<>* prefixNode = n->first_node("Prefix");
					while (prefixNode != nullptr) {
						const char* prefix = prefixNode->value();
						// If recursing, queue a sub-request, otherwise add the common prefix to the result.
						if (maxDepth > 0) {
							// If there is no recurse filter or the filter returns true then start listing the subfolder
							if (!recurseFilter || recurseFilter(prefix)) {
								subLists.push_back(bstore->listObjectsStream(
								    bucket, results, prefix, delimiter, maxDepth - 1, recurseFilter));
							}
							// Since prefix will not be in the final listResult below we have to set lastFile here in
							// case it's greater than the last object
							lastFile = prefix;
						} else {
							listResult.commonPrefixes.push_back(prefix);
						}

						prefixNode = prefixNode->next_sibling("Prefix");
					}
				}

				n = n->next_sibling();
			}

			results.send(listResult);

			if (more) {
				// lastFile will be the last commonprefix for which a sublist was started, if any.
				// If there are any objects and the last one is greater than lastFile then make it the new lastFile.
				if (!listResult.objects.empty() && lastFile < listResult.objects.back().name) {
					lastFile = listResult.objects.back().name;
				}
				// If there are any common prefixes and the last one is greater than lastFile then make it the new
				// lastFile.
				if (!listResult.commonPrefixes.empty() && lastFile < listResult.commonPrefixes.back()) {
					lastFile = listResult.commonPrefixes.back();
				}

				// If lastFile is empty at this point, something has gone wrong.
				if (lastFile.empty()) {
					TraceEvent(SevWarn, "S3BlobStoreEndpointListNoNextMarker")
					    .suppressFor(60)
					    .detail("Resource", fullResource);
					throw http_bad_response();
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "S3BlobStoreEndpointListResultParseError")
				    .errorUnsuppressed(e)
				    .suppressFor(60)
				    .detail("Resource", fullResource);
			throw http_bad_response();
		}
	}

	wait(waitForAll(subLists));

	return Void();
}

Future<Void> S3BlobStoreEndpoint::listObjectsStream(std::string const& bucket,
                                                    PromiseStream<ListResult> results,
                                                    Optional<std::string> prefix,
                                                    Optional<char> delimiter,
                                                    int maxDepth,
                                                    std::function<bool(std::string const&)> recurseFilter) {
	return listObjectsStream_impl(
	    Reference<S3BlobStoreEndpoint>::addRef(this), bucket, results, prefix, delimiter, maxDepth, recurseFilter);
}

ACTOR Future<S3BlobStoreEndpoint::ListResult> listObjects_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                               std::string bucket,
                                                               Optional<std::string> prefix,
                                                               Optional<char> delimiter,
                                                               int maxDepth,
                                                               std::function<bool(std::string const&)> recurseFilter) {
	state S3BlobStoreEndpoint::ListResult results;
	state PromiseStream<S3BlobStoreEndpoint::ListResult> resultStream;
	state Future<Void> done =
	    bstore->listObjectsStream(bucket, resultStream, prefix, delimiter, maxDepth, recurseFilter);
	// Wrap done in an actor which sends end_of_stream because list does not so that many lists can write to the same
	// stream
	done = map(done, [=](Void) mutable {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	try {
		loop {
			choose {
				// Throw if done throws, otherwise don't stop until end_of_stream
				when(wait(done)) {
					done = Never();
				}

				when(S3BlobStoreEndpoint::ListResult info = waitNext(resultStream.getFuture())) {
					results.commonPrefixes.insert(
					    results.commonPrefixes.end(), info.commonPrefixes.begin(), info.commonPrefixes.end());
					results.objects.insert(results.objects.end(), info.objects.begin(), info.objects.end());
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}

	return results;
}

Future<S3BlobStoreEndpoint::ListResult> S3BlobStoreEndpoint::listObjects(
    std::string const& bucket,
    Optional<std::string> prefix,
    Optional<char> delimiter,
    int maxDepth,
    std::function<bool(std::string const&)> recurseFilter) {
	return listObjects_impl(
	    Reference<S3BlobStoreEndpoint>::addRef(this), bucket, prefix, delimiter, maxDepth, recurseFilter);
}

ACTOR Future<std::vector<std::string>> listBuckets_impl(Reference<S3BlobStoreEndpoint> bstore) {
	state std::string resource = "/?marker=";
	state std::string lastName;
	state bool more = true;
	state std::vector<std::string> buckets;

	while (more) {
		wait(bstore->concurrentLists.take());
		state FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		HTTP::Headers headers;
		state std::string fullResource = resource + lastName;
		Reference<HTTP::IncomingResponse> r =
		    wait(bstore->doRequest("GET", fullResource, headers, nullptr, 0, { 200 }));
		listReleaser.release();

		try {
			xml_document<> doc;

			// Copy content because rapidxml will modify it during parse
			std::string content = r->data.content;
			doc.parse<0>((char*)content.c_str());

			// There should be exactly one node
			xml_node<>* result = doc.first_node();
			if (result == nullptr || strcmp(result->name(), "ListAllMyBucketsResult") != 0) {
				throw http_bad_response();
			}

			more = false;
			xml_node<>* truncated = result->first_node("IsTruncated");
			if (truncated != nullptr && strcmp(truncated->value(), "true") == 0) {
				more = true;
			}

			xml_node<>* bucketsNode = result->first_node("Buckets");
			if (bucketsNode != nullptr) {
				xml_node<>* bucketNode = bucketsNode->first_node("Bucket");
				while (bucketNode != nullptr) {
					xml_node<>* nameNode = bucketNode->first_node("Name");
					if (nameNode == nullptr) {
						throw http_bad_response();
					}
					const char* name = nameNode->value();
					buckets.push_back(name);

					bucketNode = bucketNode->next_sibling("Bucket");
				}
			}

			if (more) {
				lastName = buckets.back();
			}

		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "S3BlobStoreEndpointListBucketResultParseError")
				    .errorUnsuppressed(e)
				    .suppressFor(60)
				    .detail("Resource", fullResource);
			throw http_bad_response();
		}
	}

	return buckets;
}

Future<std::vector<std::string>> S3BlobStoreEndpoint::listBuckets() {
	return listBuckets_impl(Reference<S3BlobStoreEndpoint>::addRef(this));
}

std::string S3BlobStoreEndpoint::hmac_sha1(Credentials const& creds, std::string const& msg) {
	std::string key = creds.secret;

	// Hash key to shorten it if it is longer than SHA1 block size
	if (key.size() > 64) {
		key = SHA1::from_string(key);
	}

	// Pad key up to SHA1 block size if needed
	key.append(64 - key.size(), '\0');

	std::string kipad = key;
	for (int i = 0; i < 64; ++i)
		kipad[i] ^= '\x36';

	std::string kopad = key;
	for (int i = 0; i < 64; ++i)
		kopad[i] ^= '\x5c';

	kipad.append(msg);
	std::string hkipad = SHA1::from_string(kipad);
	kopad.append(hkipad);
	return SHA1::from_string(kopad);
}

std::string sha256_hex(std::string str) {
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, str.c_str(), str.size());
	SHA256_Final(hash, &sha256);
	std::stringstream ss;
	for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
	}
	return ss.str();
}

std::string hmac_sha256_hex(std::string key, std::string msg) {
	unsigned char hash[32];

	HMAC_CTX* hmac = HMAC_CTX_new();
	HMAC_Init_ex(hmac, &key[0], key.length(), EVP_sha256(), NULL);
	HMAC_Update(hmac, (unsigned char*)&msg[0], msg.length());
	unsigned int len = 32;
	HMAC_Final(hmac, hash, &len);
	HMAC_CTX_free(hmac);

	std::stringstream ss;
	ss << std::hex << std::setfill('0');
	for (int i = 0; i < len; i++) {
		ss << std::hex << std::setw(2) << (unsigned int)hash[i];
	}
	return (ss.str());
}

std::string hmac_sha256(std::string key, std::string msg) {
	unsigned char hash[32];

	HMAC_CTX* hmac = HMAC_CTX_new();
	HMAC_Init_ex(hmac, &key[0], key.length(), EVP_sha256(), NULL);
	HMAC_Update(hmac, (unsigned char*)&msg[0], msg.length());
	unsigned int len = 32;
	HMAC_Final(hmac, hash, &len);
	HMAC_CTX_free(hmac);

	std::stringstream ss;
	ss << std::setfill('0');
	for (int i = 0; i < len; i++) {
		ss << hash[i];
	}
	return (ss.str());
}

// Date and Time parameters are used for unit testing
void S3BlobStoreEndpoint::setV4AuthHeaders(std::string const& verb,
                                           std::string const& resource,
                                           HTTP::Headers& headers,
                                           std::string date,
                                           std::string datestamp) {
	if (!credentials.present()) {
		return;
	}
	Credentials creds = credentials.get();
	// std::cout << "========== Starting===========" << std::endl;
	std::string accessKey = creds.key;
	std::string secretKey = creds.secret;
	// Create a date for headers and the credential string
	std::string amzDate;
	std::string dateStamp;
	if (date.empty() || datestamp.empty()) {
		time_t ts;
		time(&ts);
		char dateBuf[20];
		// ISO 8601 format YYYYMMDD'T'HHMMSS'Z'
		strftime(dateBuf, 20, "%Y%m%dT%H%M%SZ", gmtime(&ts));
		amzDate = dateBuf;
		strftime(dateBuf, 20, "%Y%m%d", gmtime(&ts));
		dateStamp = dateBuf;
	} else {
		amzDate = date;
		dateStamp = datestamp;
	}

	// ************* TASK 1: CREATE A CANONICAL REQUEST *************
	// Create canonical URI--the part of the URI from domain to query string (use '/' if no path)
	std::vector<std::string> queryParameters;
	std::string canonicalURI = canonicalizeURI(resource, queryParameters);

	std::string canonicalQueryString;
	if (!queryParameters.empty()) {
		std::sort(queryParameters.begin(), queryParameters.end());
		canonicalQueryString = boost::algorithm::join(queryParameters, "&");
	}

	using namespace boost::algorithm;
	// Create the canonical headers and signed headers
	ASSERT(!headers["Host"].empty());
	// Using unsigned payload here and adding content-md5 to the signed headers. It may be better to also include sha256
	// sum for added security.
	headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";
	headers["x-amz-date"] = amzDate;
	std::vector<std::pair<std::string, std::string>> headersList;
	headersList.push_back({ "host", trim_copy(headers["Host"]) + "\n" });
	if (headers.find("Content-Type") != headers.end())
		headersList.push_back({ "content-type", trim_copy(headers["Content-Type"]) + "\n" });
	if (headers.find("Content-MD5") != headers.end())
		headersList.push_back({ "content-md5", trim_copy(headers["Content-MD5"]) + "\n" });
	for (auto h : headers) {
		if (StringRef(h.first).startsWith("x-amz"_sr))
			headersList.push_back({ to_lower_copy(h.first), trim_copy(h.second) + "\n" });
	}
	std::sort(headersList.begin(), headersList.end());
	std::string canonicalHeaders;
	std::string signedHeaders;
	for (auto& i : headersList) {
		canonicalHeaders += i.first + ":" + i.second;
		signedHeaders += i.first + ";";
	}
	signedHeaders.pop_back();
	std::string canonicalRequest = verb + "\n" + canonicalURI + "\n" + canonicalQueryString + "\n" + canonicalHeaders +
	                               "\n" + signedHeaders + "\n" + headers["x-amz-content-sha256"];

	// ************* TASK 2: CREATE THE STRING TO SIGN*************
	std::string algorithm = "AWS4-HMAC-SHA256";
	std::string credentialScope = dateStamp + "/" + region + "/s3/" + "aws4_request";
	std::string stringToSign =
	    algorithm + "\n" + amzDate + "\n" + credentialScope + "\n" + sha256_hex(canonicalRequest);

	// ************* TASK 3: CALCULATE THE SIGNATURE *************
	// Create the signing key using the function defined above.
	std::string signingKey =
	    hmac_sha256(hmac_sha256(hmac_sha256(hmac_sha256("AWS4" + secretKey, dateStamp), region), "s3"), "aws4_request");
	// Sign the string_to_sign using the signing_key
	std::string signature = hmac_sha256_hex(signingKey, stringToSign);
	// ************* TASK 4: ADD SIGNING INFORMATION TO THE Header *************
	std::string authorizationHeader = algorithm + " " + "Credential=" + accessKey + "/" + credentialScope + ", " +
	                                  "SignedHeaders=" + signedHeaders + ", " + "Signature=" + signature;
	headers["Authorization"] = authorizationHeader;
}

void S3BlobStoreEndpoint::setAuthHeaders(std::string const& verb, std::string const& resource, HTTP::Headers& headers) {
	if (!credentials.present()) {
		return;
	}
	Credentials creds = credentials.get();

	std::string& date = headers["Date"];

	char dateBuf[64];
	time_t ts;
	time(&ts);
	strftime(dateBuf, 64, "%a, %d %b %Y %H:%M:%S GMT", gmtime(&ts));
	date = dateBuf;

	std::string msg;
	msg.append(verb);
	msg.append("\n");
	auto contentMD5 = headers.find("Content-MD5");
	if (contentMD5 != headers.end())
		msg.append(contentMD5->second);
	msg.append("\n");
	auto contentType = headers.find("Content-Type");
	if (contentType != headers.end())
		msg.append(contentType->second);
	msg.append("\n");
	msg.append(date);
	msg.append("\n");
	for (auto h : headers) {
		StringRef name = h.first;
		if (name.startsWith("x-amz"_sr) || name.startsWith("x-icloud"_sr)) {
			msg.append(h.first);
			msg.append(":");
			msg.append(h.second);
			msg.append("\n");
		}
	}

	msg.append(resource);
	if (verb == "GET") {
		size_t q = resource.find_last_of('?');
		if (q != resource.npos)
			msg.resize(msg.size() - (resource.size() - q));
	}

	std::string sig = base64::encoder::from_string(hmac_sha1(creds, msg));
	// base64 encoded blocks end in \n so remove it.
	sig.resize(sig.size() - 1);
	std::string auth = "AWS ";
	auth.append(creds.key);
	auth.append(":");
	auth.append(sig);
	headers["Authorization"] = auth;
}

ACTOR Future<std::string> readEntireFile_impl(Reference<S3BlobStoreEndpoint> bstore,
                                              std::string bucket,
                                              std::string object) {
	wait(bstore->requestRateRead->getAllowance(1));

	std::string resource = constructResourcePath(bstore, bucket, object);
	HTTP::Headers headers;
	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("GET", resource, headers, nullptr, 0, { 200, 404 }));
	if (r->code == 404)
		throw file_not_found();
	return r->data.content;
}

Future<std::string> S3BlobStoreEndpoint::readEntireFile(std::string const& bucket, std::string const& object) {
	return readEntireFile_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> writeEntireFileFromBuffer_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                  std::string bucket,
                                                  std::string object,
                                                  UnsentPacketQueue* pContent,
                                                  int contentLen,
                                                  std::string contentMD5) {
	if (contentLen > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	wait(bstore->requestRateWrite->getAllowance(1));
	wait(bstore->concurrentUploads.take());
	state FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = constructResourcePath(bstore, bucket, object);
	HTTP::Headers headers;
	// Send MD5 sum for content so blobstore can verify it
	headers["Content-MD5"] = contentMD5;
	if (!CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE.empty())
		headers["x-amz-server-side-encryption"] = CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE;
	state Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, { 200 }));

	// For uploads, Blobstore returns an MD5 sum of uploaded content so check it.
	if (!HTTP::verifyMD5(&r->data, false, contentMD5))
		throw checksum_failed();

	return Void();
}

ACTOR Future<Void> writeEntireFile_impl(Reference<S3BlobStoreEndpoint> bstore,
                                        std::string bucket,
                                        std::string object,
                                        std::string content) {
	state UnsentPacketQueue packets;
	if (content.size() > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	PacketWriter pw(packets.getWriteBuffer(content.size()), nullptr, Unversioned());
	pw.serializeBytes(content);

	// Yield because we may have just had to copy several MB's into packet buffer chain and next we have to calculate an
	// MD5 sum of it.
	// TODO:  If this actor is used to send large files then combine the summing and packetization into a loop with a
	// yield() every 20k or so.
	wait(yield());

	MD5_CTX sum;
	::MD5_Init(&sum);
	::MD5_Update(&sum, content.data(), content.size());
	std::string sumBytes;
	sumBytes.resize(16);
	::MD5_Final((unsigned char*)sumBytes.data(), &sum);
	std::string contentMD5 = base64::encoder::from_string(sumBytes);
	contentMD5.resize(contentMD5.size() - 1);

	wait(writeEntireFileFromBuffer_impl(bstore, bucket, object, &packets, content.size(), contentMD5));
	return Void();
}

Future<Void> S3BlobStoreEndpoint::writeEntireFile(std::string const& bucket,
                                                  std::string const& object,
                                                  std::string const& content) {
	return writeEntireFile_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, content);
}

Future<Void> S3BlobStoreEndpoint::writeEntireFileFromBuffer(std::string const& bucket,
                                                            std::string const& object,
                                                            UnsentPacketQueue* pContent,
                                                            int contentLen,
                                                            std::string const& contentMD5) {
	return writeEntireFileFromBuffer_impl(
	    Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, pContent, contentLen, contentMD5);
}

ACTOR Future<int> readObject_impl(Reference<S3BlobStoreEndpoint> bstore,
                                  std::string bucket,
                                  std::string object,
                                  void* data,
                                  int length,
                                  int64_t offset) {
	if (length <= 0)
		return 0;
	wait(bstore->requestRateRead->getAllowance(1));

	std::string resource = constructResourcePath(bstore, bucket, object);
	HTTP::Headers headers;
	headers["Range"] = format("bytes=%lld-%lld", offset, offset + length - 1);
	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("GET", resource, headers, nullptr, 0, { 200, 206, 404 }));
	if (r->code == 404)
		throw file_not_found();
	if (r->data.contentLen !=
	    r->data.content.size()) // Double check that this wasn't a header-only response, probably unnecessary
		throw io_error();
	// Copy the output bytes, server could have sent more or less bytes than requested so copy at most length bytes
	memcpy(data, r->data.content.data(), std::min<int64_t>(r->data.contentLen, length));
	return r->data.contentLen;
}

Future<int> S3BlobStoreEndpoint::readObject(std::string const& bucket,
                                            std::string const& object,
                                            void* data,
                                            int length,
                                            int64_t offset) {
	return readObject_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, data, length, offset);
}

ACTOR static Future<std::string> beginMultiPartUpload_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                           std::string bucket,
                                                           std::string object) {
	wait(bstore->requestRateWrite->getAllowance(1));

	std::string resource = constructResourcePath(bstore, bucket, object);
	resource += "?uploads";
	HTTP::Headers headers;
	if (!CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE.empty())
		headers["x-amz-server-side-encryption"] = CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE;
	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("POST", resource, headers, nullptr, 0, { 200 }));

	try {
		xml_document<> doc;
		// Copy content because rapidxml will modify it during parse
		std::string content = r->data.content;

		doc.parse<0>((char*)content.c_str());

		// There should be exactly one node
		xml_node<>* result = doc.first_node();
		if (result != nullptr && strcmp(result->name(), "InitiateMultipartUploadResult") == 0) {
			xml_node<>* id = result->first_node("UploadId");
			if (id != nullptr) {
				return id->value();
			}
		}
	} catch (...) {
	}
	throw http_bad_response();
}

Future<std::string> S3BlobStoreEndpoint::beginMultiPartUpload(std::string const& bucket, std::string const& object) {
	return beginMultiPartUpload_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<std::string> uploadPart_impl(Reference<S3BlobStoreEndpoint> bstore,
                                          std::string bucket,
                                          std::string object,
                                          std::string uploadID,
                                          unsigned int partNumber,
                                          UnsentPacketQueue* pContent,
                                          int contentLen,
                                          std::string contentMD5) {
	wait(bstore->requestRateWrite->getAllowance(1));
	wait(bstore->concurrentUploads.take());
	state FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = constructResourcePath(bstore, bucket, object);
	resource += format("?partNumber=%d&uploadId=%s", partNumber, uploadID.c_str());
	HTTP::Headers headers;
	// Send MD5 sum for content so blobstore can verify it
	headers["Content-MD5"] = contentMD5;
	state Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, { 200 }));
	// TODO:  In the event that the client times out just before the request completes (so the client is unaware) then
	// the next retry will see error 400.  That could be detected and handled gracefully by retrieving the etag for the
	// successful request.

	// For uploads, Blobstore returns an MD5 sum of uploaded content so check it.
	if (!HTTP::verifyMD5(&r->data, false, contentMD5))
		throw checksum_failed();

	// No etag -> bad response.
	std::string etag = r->data.headers["ETag"];
	if (etag.empty())
		throw http_bad_response();

	return etag;
}

Future<std::string> S3BlobStoreEndpoint::uploadPart(std::string const& bucket,
                                                    std::string const& object,
                                                    std::string const& uploadID,
                                                    unsigned int partNumber,
                                                    UnsentPacketQueue* pContent,
                                                    int contentLen,
                                                    std::string const& contentMD5) {
	return uploadPart_impl(Reference<S3BlobStoreEndpoint>::addRef(this),
	                       bucket,
	                       object,
	                       uploadID,
	                       partNumber,
	                       pContent,
	                       contentLen,
	                       contentMD5);
}

ACTOR Future<Void> finishMultiPartUpload_impl(Reference<S3BlobStoreEndpoint> bstore,
                                              std::string bucket,
                                              std::string object,
                                              std::string uploadID,
                                              S3BlobStoreEndpoint::MultiPartSetT parts) {
	state UnsentPacketQueue part_list; // NonCopyable state var so must be declared at top of actor
	wait(bstore->requestRateWrite->getAllowance(1));

	std::string manifest = "<CompleteMultipartUpload>";
	for (auto& p : parts)
		manifest += format("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>\n", p.first, p.second.c_str());
	manifest += "</CompleteMultipartUpload>";

	std::string resource = constructResourcePath(bstore, bucket, object);
	resource += format("?uploadId=%s", uploadID.c_str());
	HTTP::Headers headers;
	PacketWriter pw(part_list.getWriteBuffer(manifest.size()), nullptr, Unversioned());
	pw.serializeBytes(manifest);
	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("POST", resource, headers, &part_list, manifest.size(), { 200 }));
	// TODO:  In the event that the client times out just before the request completes (so the client is unaware) then
	// the next retry will see error 400.  That could be detected and handled gracefully by HEAD'ing the object before
	// upload to get its (possibly nonexistent) eTag, then if an error 400 is seen then retrieve the eTag again and if
	// it has changed then consider the finish complete.
	return Void();
}

Future<Void> S3BlobStoreEndpoint::finishMultiPartUpload(std::string const& bucket,
                                                        std::string const& object,
                                                        std::string const& uploadID,
                                                        MultiPartSetT const& parts) {
	return finishMultiPartUpload_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, uploadID, parts);
}

TEST_CASE("/backup/s3/v4headers") {
	Optional creds(
	    S3BlobStoreEndpoint::Credentials{ "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "" });

	// GET without query parameters
	{
		S3BlobStoreEndpoint s3("s3.amazonaws.com", "443", "amazonaws", "proxy", "port", creds);
		std::string verb("GET");
		std::string resource("/test.txt");
		HTTP::Headers headers;
		headers["Host"] = "s3.amazonaws.com";
		s3.setV4AuthHeaders(verb, resource, headers, "20130524T000000Z", "20130524");
		ASSERT(headers["Authorization"] ==
		       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/amazonaws/s3/aws4_request, "
		       "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
		       "Signature=c6037f4b174f2019d02d7085a611cef8adfe1efe583e220954dc85d59cd31ba3");
		ASSERT(headers["x-amz-date"] == "20130524T000000Z");
	}

	// GET with query parameters
	{
		S3BlobStoreEndpoint s3("s3.amazonaws.com", "443", "amazonaws", "proxy", "port", creds);
		std::string verb("GET");
		std::string resource("/test/examplebucket?Action=DescribeRegions&Version=2013-10-15");
		HTTP::Headers headers;
		headers["Host"] = "s3.amazonaws.com";
		s3.setV4AuthHeaders(verb, resource, headers, "20130524T000000Z", "20130524");
		ASSERT(headers["Authorization"] ==
		       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/amazonaws/s3/aws4_request, "
		       "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
		       "Signature=426f04e71e191fbc30096c306fe1b11ce8f026a7be374541862bbee320cce71c");
		ASSERT(headers["x-amz-date"] == "20130524T000000Z");
	}

	// POST
	{
		S3BlobStoreEndpoint s3("s3.us-west-2.amazonaws.com", "443", "us-west-2", "proxy", "port", creds);
		std::string verb("POST");
		std::string resource("/simple.json");
		HTTP::Headers headers;
		headers["Host"] = "s3.us-west-2.amazonaws.com";
		headers["Content-Type"] = "Application/x-amz-json-1.0";
		s3.setV4AuthHeaders(verb, resource, headers, "20130524T000000Z", "20130524");
		ASSERT(headers["Authorization"] ==
		       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-west-2/s3/aws4_request, "
		       "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, "
		       "Signature=cf095e36bed9cd3139c2e8b3e20c296a79d8540987711bf3a0d816b19ae00314");
		ASSERT(headers["x-amz-date"] == "20130524T000000Z");
		ASSERT(headers["Host"] == "s3.us-west-2.amazonaws.com");
		ASSERT(headers["Content-Type"] == "Application/x-amz-json-1.0");
	}

	return Void();
}

TEST_CASE("/backup/s3/guess_region") {
	std::string url = "blobstore://s3.us-west-2.amazonaws.com/resource_name?bucket=bucket_name&sa=1";

	std::string resource;
	std::string error;
	IBlobStoreEndpoint::ParametersT parameters;
	Reference<IBlobStoreEndpoint> s3 = IBlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);
	ASSERT(s3->getRegion() == "us-west-2");

	url = "blobstore://s3.us-west-2.amazonaws.com/resource_name?bucket=bucket_name&sc=922337203685477580700";
	try {
		s3 = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);
		ASSERT(false); // not reached
	} catch (Error& e) {
		// conversion of 922337203685477580700 to long int will overflow
		ASSERT_EQ(e.code(), error_code_backup_invalid_url);
	}
	return Void();
}
