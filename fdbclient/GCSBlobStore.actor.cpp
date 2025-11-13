/*
 * GCSBlobStore.actor.cpp
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

#include "fdbclient/GCSBlobStore.h"
#include "fdbrpc/HTTP.h"
#include "fdbclient/json_spirit/json_spirit_reader_template.h"
#include "fdbrpc/Stats.h"
#include "fdbclient/JSONDoc.h"
#include <fstream>

#include "flow/actorcompiler.h" // has to be last include
#include "libb64/encode.h"

#include <openssl/md5.h>

Optional<GCSBlobStoreEndpoint::Credentials> parseGcpCredentials(Optional<StringRef> const& credString) {
	if (credString.present()) {
		return GCSBlobStoreEndpoint::Credentials{ credString.get().toString() };
	}
	return Optional<GCSBlobStoreEndpoint::Credentials>();
}

GCSBlobStoreEndpoint::GCSBlobStoreEndpoint(std::string const& host,
                                           std::string const& service,
                                           Optional<std::string> const& proxyHost,
                                           Optional<std::string> const& proxyPort,
                                           Optional<StringRef> const& creds,
                                           BlobKnobs const& knobs,
                                           HTTP::Headers extraHeaders)
  : IBlobStoreEndpoint(host, service, "auto", proxyHost, proxyPort, knobs, extraHeaders),
    credentials(parseGcpCredentials(creds)) {
	// GCS only works with dynamic credential fetching
	if (!credentials.present() || !credentials.get().token.empty()) {
		throw backup_auth_missing();
	}
}

std::string GCSBlobStoreEndpoint::normalizeURIForRemoteRequest(const std::string& resource) {
	return resource;
}

std::string GCSBlobStoreEndpoint::getResourceURL(std::string resource, std::string params) const {
	std::string hostPort = host;
	if (!service.empty()) {
		hostPort.append(":");
		hostPort.append(service);
	}

	std::string r = format("blobstore://@%s/%s", hostPort.c_str(), resource.c_str());

	// Get params that are deviations from knob defaults
	std::string knobParams = knobs.getURLParameters();
	if (!knobParams.empty()) {
		if (!params.empty()) {
			params.append("&");
		}
		params.append(knobParams);
	}

	if (!params.empty())
		r.append("?").append(params);

	return r;
}

ACTOR Future<Void> updateSecret_impl(Reference<GCSBlobStoreEndpoint> b) {
	std::vector<std::string>* pFiles = (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
	if (pFiles == nullptr)
		return Void();

	state std::vector<Future<Optional<json_spirit::mObject>>> reads;
	for (auto& f : *pFiles)
		reads.push_back(tryReadJSONFile(f));

	wait(waitForAll(reads));

	std::string credentialsFileKey = "@" + b->host;

	int invalid = 0;
	TraceEvent("GCSBlobStoreEndpointReadingSecrets");

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
				GCSBlobStoreEndpoint::Credentials creds = b->credentials.get();
				std::string token;
				if (account.tryGet("token", token)) {
					creds.token = token;
					b->credentials = creds;
					TraceEvent("GCSBlobStoreEndpointUpdatedSecret").detail("Token", token);
					return Void();
				}
			}
		}
	}

	// If any sources were invalid
	if (invalid > 0)
		throw backup_auth_unreadable();

	// All sources were valid but didn't contain the desired info
	throw backup_auth_missing();
}

Future<Void> GCSBlobStoreEndpoint::updateSecret() {
	return updateSecret_impl(Reference<GCSBlobStoreEndpoint>::addRef(this));
}

bool GCSBlobStoreEndpoint::lookupSecretOnEachRequest() {
	return true;
}

void GCSBlobStoreEndpoint::setAllRequestHeaders(const std::string& verb,
                                                const std::string& resource,
                                                HTTP::Headers& headers,
                                                std::string date,
                                                std::string datestamp) {
	headers["Accept"] = "application/json";
	if (credentials.present()) {
		headers["Authorization"] = "Bearer " + credentials.get().token;
	}
}

std::string constructResourceURL(std::string const& bucket,
                                 std::string const& object,
                                 std::string const& queryStrings) {
	return format("/storage/v1/b/%s/o/%s?%s",
	              HTTP::gcpPathParamUrlEncode(bucket).c_str(),
	              HTTP::gcpPathParamUrlEncode(object).c_str(),
	              queryStrings.c_str());
}
ACTOR Future<int> readObject_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                  std::string bucket,
                                  std::string object,
                                  void* data,
                                  int length,
                                  int64_t offset) {
	if (length <= 0)
		return 0;
	wait(bstore->requestRateRead->getAllowance(1));

	std::string resource = constructResourceURL(bucket, object, "alt=media");
	HTTP::Headers headers;
	if (offset > 0 || length > 0) {
		headers["Range"] = format("bytes=%lld-%lld", offset, offset + length - 1);
	}
	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("GET", resource, headers, nullptr, 0, { 200, 206, 404 }));

	if (r->code == 404)
		throw file_not_found();
	if (r->data.contentLen != r->data.content.size())
		throw io_error();

	memcpy(data, r->data.content.data(), std::min<int64_t>(r->data.contentLen, length));
	return r->data.contentLen;
}

Future<int> GCSBlobStoreEndpoint::readObject(std::string const& bucket,
                                             std::string const& object,
                                             void* data,
                                             int length,
                                             int64_t offset) {
	return readObject_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object, data, length, offset);
}

ACTOR Future<std::string> readEntireFile_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                              std::string bucket,
                                              std::string object) {
	wait(bstore->requestRateRead->getAllowance(1));
	std::string resource = constructResourceURL(bucket, object, "alt=media");
	HTTP::Headers headers;
	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("GET", resource, headers, nullptr, 0, { 200, 206, 404 }));
	if (r->code == 404)
		throw file_not_found();
	return r->data.content;
}

Future<std::string> GCSBlobStoreEndpoint::readEntireFile(std::string const& bucket, std::string const& object) {
	return readEntireFile_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<bool> bucketExists_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket) {
	wait(bstore->requestRateRead->getAllowance(1));
	std::string resource = format("/storage/v1/b/%s", HTTP::gcpPathParamUrlEncode(bucket).c_str());
	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("GET", resource, {}, nullptr, 0, { 200, 404 }));
	return r->code == 200;
}

Future<bool> GCSBlobStoreEndpoint::bucketExists(std::string const& bucket) {
	return bucketExists_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<Void> createBucket_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket) {
	bool exists = wait(bstore->bucketExists(bucket));
	if (exists) {
		TraceEvent(SevInfo, "GCSBucketAlreadyExists").detail("Bucket", bucket);
		return Void();
	}

	throw not_implemented();
}

Future<Void> GCSBlobStoreEndpoint::createBucket(std::string const& bucket) {
	return createBucket_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<std::vector<std::string>> listBuckets_impl(Reference<GCSBlobStoreEndpoint> bstore) {
	wait(bstore->requestRateRead->getAllowance(1));
	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("GET", "/storage/v1/b", {}, nullptr, 0, { 200 }));

	std::string response(r->data.content.begin(), r->data.content.end());

	json_spirit::mValue json;
	json_spirit::read_string(response, json);

	if (json.type() != json_spirit::obj_type)
		throw http_bad_response();

	json_spirit::mObject obj = json.get_obj();
	std::vector<std::string> buckets;

	auto items = obj.find("items");
	if (items != obj.end() && items->second.type() == json_spirit::array_type) {
		json_spirit::mArray itemsArray = items->second.get_array();
		for (const auto& item : itemsArray) {
			if (item.type() != json_spirit::obj_type)
				continue;

			json_spirit::mObject itemObj = item.get_obj();
			auto nameIterable = itemObj.find("name");
			if (nameIterable != itemObj.end() && nameIterable->second.type() == json_spirit::str_type) {
				buckets.push_back(nameIterable->second.get_str());
			}
		}
	}

	return buckets;
}

Future<std::vector<std::string>> GCSBlobStoreEndpoint::listBuckets() {
	return listBuckets_impl(Reference<GCSBlobStoreEndpoint>::addRef(this));
}

ACTOR Future<bool> objectExists_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	wait(bstore->requestRateRead->getAllowance(1));
	std::string resource = format("/storage/v1/b/%s/o/%s",
	                              HTTP::gcpPathParamUrlEncode(bucket).c_str(),
	                              HTTP::gcpPathParamUrlEncode(object).c_str());
	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("GET", resource, {}, nullptr, 0, { 200, 404 }));
	return r->code == 200;
}

Future<bool> GCSBlobStoreEndpoint::objectExists(std::string const& bucket, std::string const& object) {
	return objectExists_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<int64_t> objectSize_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	wait(bstore->requestRateRead->getAllowance(1));

	std::string resource = constructResourceURL(bucket, object, "fields=size");
	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("GET", resource, {}, nullptr, 0, { 200, 404 }));
	if (r->code == 404)
		throw file_not_found();
	std::string response(r->data.content.begin(), r->data.content.end());

	json_spirit::mValue json;
	json_spirit::read_string(response, json);

	if (json.type() != json_spirit::obj_type)
		throw http_bad_response();

	json_spirit::mObject obj = json.get_obj();
	auto sizeIt = obj.find("size");
	if (sizeIt != obj.end() && sizeIt->second.type() == json_spirit::str_type)
		return std::stoll(sizeIt->second.get_str());

	throw http_bad_response();
}

Future<int64_t> GCSBlobStoreEndpoint::objectSize(std::string const& bucket, std::string const& object) {
	return objectSize_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteObject_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	wait(bstore->requestRateDelete->getAllowance(1));

	std::string resource = format("/storage/v1/b/%s/o/%s",
	                              HTTP::gcpPathParamUrlEncode(bucket).c_str(),
	                              HTTP::gcpPathParamUrlEncode(object).c_str());
	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("DELETE", resource, {}, nullptr, 0, { 200, 204, 404 }));
	if (r->code == 404)
		throw file_not_found();

	return Void();
}

Future<Void> GCSBlobStoreEndpoint::deleteObject(std::string const& bucket, std::string const& object) {
	return deleteObject_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteRecursively_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                          std::string bucket,
                                          std::string prefix,
                                          int* pNumDeleted,
                                          int64_t* pBytesDeleted) {
	state PromiseStream<GCSBlobStoreEndpoint::ListResult> resultStream;
	state Future<Void> done = bstore->listObjectsStream(bucket, resultStream, prefix, Optional<char>(), 0, nullptr);

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

				when(GCSBlobStoreEndpoint::ListResult list = waitNext(resultStream.getFuture())) {
					for (auto& object : list.objects) {
						deleteFutures.push_back(map(bstore->deleteObject(bucket, object.name), [=](Void) {
							if (pNumDeleted != nullptr) {
								++(*pNumDeleted);
							}
							if (pBytesDeleted != nullptr) {
								(*pBytesDeleted) += object.size;
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

	wait(done);
	while (deleteFutures.size() > 0) {
		wait(deleteFutures.front());
		deleteFutures.pop_front();
	}
	return Void();
}

Future<Void> GCSBlobStoreEndpoint::deleteRecursively(std::string const& bucket,
                                                     std::string prefix,
                                                     int* pNumDeleted,
                                                     int64_t* pBytesDeleted) {
	return deleteRecursively_impl(
	    Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, prefix, pNumDeleted, pBytesDeleted);
}

void checkMd5Hash(Reference<HTTP::IncomingResponse> response, std::string const& contentMD5) {
	json_spirit::mValue respJson;
	if (!json_spirit::read_string(response->data.content, respJson))
		throw http_request_failed();

	auto obj = respJson.get_obj();
	auto it = obj.find("md5Hash");
	if (it == obj.end() || it->second.get_str() != contentMD5)
		throw checksum_failed();
}

ACTOR Future<Void> writeEntireFileFromBuffer_impl(Reference<GCSBlobStoreEndpoint> bstore,
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

	std::string resource = format("/upload/storage/v1/b/%s/o?uploadType=media&name=%s",
	                              HTTP::gcpPathParamUrlEncode(bucket).c_str(),
	                              HTTP::gcpPathParamUrlEncode(object).c_str());

	HTTP::Headers headers;
	headers["Content-Type"] = "application/octet-stream";
	headers["Content-Length"] = std::to_string(contentLen);

	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("POST", resource, headers, pContent, contentLen, { 200, 201 }));

	if (r->code != 200 && r->code != 201)
		throw http_request_failed();

	checkMd5Hash(r, contentMD5);
	return Void();
}

Future<Void> GCSBlobStoreEndpoint::writeEntireFileFromBuffer(std::string const& bucket,
                                                             std::string const& object,
                                                             UnsentPacketQueue* pContent,
                                                             int contentLen,
                                                             std::string const& contentMD5) {
	return writeEntireFileFromBuffer_impl(
	    Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object, pContent, contentLen, contentMD5);
}

ACTOR Future<Void> writeEntireFile_impl(Reference<GCSBlobStoreEndpoint> bstore,
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

Future<Void> GCSBlobStoreEndpoint::writeEntireFile(std::string const& bucket,
                                                   std::string const& object,
                                                   std::string const& content) {
	return writeEntireFile_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object, content);
}

ACTOR Future<std::string> beginMultiPartUpload_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                                    std::string bucket,
                                                    std::string object) {
	wait(bstore->requestRateWrite->getAllowance(1));

	std::string resource = format("/upload/storage/v1/b/%s/o?uploadType=resumable&name=%s",
	                              HTTP::gcpPathParamUrlEncode(bucket).c_str(),
	                              HTTP::gcpPathParamUrlEncode(object).c_str());

	HTTP::Headers headers;
	headers["Content-Type"] = "application/octet-stream";
	headers["Content-Length"] = "0";

	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("POST", resource, headers, nullptr, 0, { 200 }));

	auto it = r->data.headers.find("Location");
	if (it == r->data.headers.end())
		throw http_bad_response();

	std::string uploadURL = it->second;
	size_t uploadIdPos = uploadURL.find("upload_id=");
	if (uploadIdPos == std::string::npos)
		throw http_bad_response();

	return uploadURL.substr(uploadIdPos + 10);
}

Future<std::string> GCSBlobStoreEndpoint::beginMultiPartUpload(std::string const& bucket, std::string const& object) {
	return beginMultiPartUpload_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

std::string constructResumableUploadURL(std::string const& bucket, std::string const& uploadID) {
	return format("/upload/storage/v1/b/%s/o?uploadType=resumable&upload_id=%s",
	              HTTP::gcpPathParamUrlEncode(bucket).c_str(),
	              HTTP::gcpPathParamUrlEncode(uploadID).c_str());
}

ACTOR Future<std::string> uploadPart_impl(Reference<GCSBlobStoreEndpoint> bstore,
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

	std::string resource = constructResumableUploadURL(bucket, uploadID);

	int64_t rangeStart = (partNumber - 1) * contentLen;
	int64_t rangeEnd = rangeStart + contentLen - 1;

	HTTP::Headers headers;
	headers["Content-Type"] = "application/octet-stream";
	headers["Content-Length"] = std::to_string(contentLen);
	headers["Content-Range"] = format("bytes %lld-%lld/*", rangeStart, rangeEnd);

	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, { 200, 308 }));

	checkMd5Hash(r, contentMD5);

	return format("part-%d", partNumber);
}

Future<std::string> GCSBlobStoreEndpoint::uploadPart(std::string const& bucket,
                                                     std::string const& object,
                                                     std::string const& uploadID,
                                                     unsigned int partNumber,
                                                     UnsentPacketQueue* pContent,
                                                     int contentLen,
                                                     std::string const& contentMD5) {
	return uploadPart_impl(Reference<GCSBlobStoreEndpoint>::addRef(this),
	                       bucket,
	                       object,
	                       uploadID,
	                       partNumber,
	                       pContent,
	                       contentLen,
	                       contentMD5);
}

ACTOR Future<Void> finishMultiPartUpload_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                              std::string bucket,
                                              std::string object,
                                              std::string uploadID,
                                              GCSBlobStoreEndpoint::MultiPartSetT parts) {
	wait(bstore->requestRateWrite->getAllowance(1));

	state int totalParts = parts.size();
	if (totalParts == 0)
		throw io_error();

	int lastPartNum = parts.rbegin()->first;
	int contentLen = 0;
	int64_t totalSize = lastPartNum * contentLen;

	std::string resource = constructResumableUploadURL(bucket, uploadID);

	HTTP::Headers headers;
	headers["Content-Length"] = "0";
	headers["Content-Range"] = format("bytes */%lld", totalSize);

	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("PUT", resource, headers, nullptr, 0, { 200, 201 }));

	return Void();
}

Future<Void> GCSBlobStoreEndpoint::finishMultiPartUpload(std::string const& bucket,
                                                         std::string const& object,
                                                         std::string const& uploadID,
                                                         MultiPartSetT const& parts) {
	return finishMultiPartUpload_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object, uploadID, parts);
}

ACTOR Future<Void> listObjectsStream_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                          std::string bucket,
                                          PromiseStream<GCSBlobStoreEndpoint::ListResult> results,
                                          Optional<std::string> prefix,
                                          Optional<char> delimiter,
                                          int maxDepth,
                                          std::function<bool(std::string const&)> recurseFilter) {
	state std::string resource =
	    format("/storage/v1/b/%s/o?maxResults=1000", HTTP::gcpPathParamUrlEncode(bucket).c_str());
	if (prefix.present())
		resource += format("&prefix=%s", prefix.get().c_str());
	if (delimiter.present())
		resource += format("&delimiter=%c", delimiter.get());

	state std::string pageToken;
	state bool more = true;
	state std::vector<Future<Void>> subLists;

	while (more) {
		wait(bstore->concurrentLists.take());
		state FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		state std::string fullResource = resource;
		if (!pageToken.empty())
			fullResource += format("&pageToken=%s", pageToken.c_str());

		Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("GET", fullResource, {}, nullptr, 0, { 200 }));
		listReleaser.release();

		std::string response(r->data.content.begin(), r->data.content.end());

		json_spirit::mValue json;
		json_spirit::read_string(response, json);

		if (json.type() != json_spirit::obj_type)
			throw http_bad_response();

		json_spirit::mObject obj = json.get_obj();
		GCSBlobStoreEndpoint::ListResult listResult;

		auto itemsIt = obj.find("items");
		if (itemsIt != obj.end() && itemsIt->second.type() == json_spirit::array_type) {
			json_spirit::mArray items = itemsIt->second.get_array();
			for (const auto& item : items) {
				if (item.type() != json_spirit::obj_type)
					throw http_bad_response();

				json_spirit::mObject itemObj = item.get_obj();
				GCSBlobStoreEndpoint::ObjectInfo objInfo;

				auto nameIterable = itemObj.find("name");
				if (nameIterable != itemObj.end() && nameIterable->second.type() == json_spirit::str_type)
					objInfo.name = nameIterable->second.get_str();
				else
					throw http_bad_response();

				auto sizeIterable = itemObj.find("size");
				if (sizeIterable != itemObj.end() && sizeIterable->second.type() == json_spirit::str_type)
					objInfo.size = std::stoll(sizeIterable->second.get_str());
				else
					throw http_bad_response();

				listResult.objects.push_back(objInfo);
			}
		}

		auto prefixesIterable = obj.find("prefixes");
		if (prefixesIterable != obj.end() && prefixesIterable->second.type() == json_spirit::array_type) {
			json_spirit::mArray prefixes = prefixesIterable->second.get_array();
			for (const auto& prefixVal : prefixes) {
				if (prefixVal.type() != json_spirit::str_type)
					throw http_bad_response();

				std::string commonPrefix = prefixVal.get_str();

				if (maxDepth > 0) {
					if (!recurseFilter || recurseFilter(commonPrefix)) {
						subLists.push_back(bstore->listObjectsStream(
						    bucket, results, commonPrefix, delimiter, maxDepth - 1, recurseFilter));
					}
				} else {
					listResult.commonPrefixes.push_back(commonPrefix);
				}
			}
		}

		results.send(listResult);

		auto nextPageIterable = obj.find("nextPageToken");
		if (nextPageIterable != obj.end() && nextPageIterable->second.type() == json_spirit::str_type) {
			pageToken = nextPageIterable->second.get_str();
			more = true;
		} else {
			more = false;
		}
	}

	wait(waitForAll(subLists));
	return Void();
}

Future<Void> GCSBlobStoreEndpoint::listObjectsStream(std::string const& bucket,
                                                     PromiseStream<ListResult> results,
                                                     Optional<std::string> prefix,
                                                     Optional<char> delimiter,
                                                     int maxDepth,
                                                     std::function<bool(std::string const&)> recurseFilter) {
	return listObjectsStream_impl(
	    Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, results, prefix, delimiter, maxDepth, recurseFilter);
}

ACTOR Future<GCSBlobStoreEndpoint::ListResult> listObjects_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                                                std::string bucket,
                                                                Optional<std::string> prefix,
                                                                Optional<char> delimiter,
                                                                int maxDepth,
                                                                std::function<bool(std::string const&)> recurseFilter) {
	state GCSBlobStoreEndpoint::ListResult results;
	state PromiseStream<GCSBlobStoreEndpoint::ListResult> resultStream;
	state Future<Void> done =
	    bstore->listObjectsStream(bucket, resultStream, prefix, delimiter, maxDepth, recurseFilter);

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

				when(GCSBlobStoreEndpoint::ListResult info = waitNext(resultStream.getFuture())) {
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

	wait(done);
	return results;
}

Future<GCSBlobStoreEndpoint::ListResult> GCSBlobStoreEndpoint::listObjects(
    std::string const& bucket,
    Optional<std::string> prefix,
    Optional<char> delimiter,
    int maxDepth,
    std::function<bool(std::string const&)> recurseFilter) {
	return listObjects_impl(
	    Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, prefix, delimiter, maxDepth, recurseFilter);
}
