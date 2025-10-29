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
#include "flow/IConnection.h"
#include "fdbclient/json_spirit/json_spirit_reader_template.h"
#include "fdbrpc/Stats.h"
#include <fstream>

#include "flow/actorcompiler.h" // has to be last include

GCSBlobStoreEndpoint::Stats GCSBlobStoreEndpoint::Stats::operator-(const Stats& rhs) {
	Stats r;
	r.requests_failed = requests_failed - rhs.requests_failed;
	r.requests_successful = requests_successful - rhs.requests_successful;
	r.bytes_sent = bytes_sent - rhs.bytes_sent;
	return r;
}

json_spirit::mObject GCSBlobStoreEndpoint::Stats::getJSON() {
	json_spirit::mObject o;
	o["requests_failed"] = requests_failed;
	o["requests_successful"] = requests_successful;
	o["bytes_sent"] = bytes_sent;
	return o;
}

GCSBlobStoreEndpoint::Stats GCSBlobStoreEndpoint::s_stats;
std::unique_ptr<GCSBlobStoreEndpoint::BlobStats> GCSBlobStoreEndpoint::blobStats;
Future<Void> GCSBlobStoreEndpoint::statsLogger = Never();

GCSBlobStoreEndpoint::BlobStats::BlobStats()
  : id(deterministicRandom()->randomUniqueID()), cc("GCSBlobStoreStats", id.toString()),
    requestsSuccessful("RequestsSuccessful", cc), requestsFailed("RequestsFailed", cc),
    newConnections("NewConnections", cc), expiredConnections("ExpiredConnections", cc),
    reusedConnections("ReusedConnections", cc), fastRetries("FastRetries", cc),
    requestLatency("GCSBlobStoreRequestLatency",
                   id,
                   CLIENT_KNOBS->BLOBSTORE_LATENCY_LOGGING_INTERVAL,
                   CLIENT_KNOBS->BLOBSTORE_LATENCY_LOGGING_ACCURACY) {}

void GCSBlobStoreEndpoint::maybeStartStatsLogger() {
	if (!blobStats && CLIENT_KNOBS->BLOBSTORE_ENABLE_LOGGING) {
		blobStats = std::make_unique<BlobStats>();
		statsLogger = blobStats->cc.traceCounters(
		    "GCSBlobStoreMetrics", blobStats->id, CLIENT_KNOBS->BLOBSTORE_STATS_LOGGING_INTERVAL, "GCSBlobStoreMetrics");
	}
}

Reference<GCSBlobStoreEndpoint> GCSBlobStoreEndpoint::fromString(const std::string& url,
                                                                  std::string* resourceFromURL,
                                                                  std::string* error,
                                                                  IBlobStoreEndpoint::ParametersT* ignored_parameters) {
	if (resourceFromURL)
		resourceFromURL->clear();

	try {
		StringRef t(url);
		StringRef prefix = t.eat("://");
		if (prefix != "gcs"_sr)
			throw format("Invalid GCS URL prefix '%s'", prefix.toString().c_str());

		// Parse host:port/resource?params
		uint8_t foundSeparator = 0;
		StringRef hostPort = t.eatAny("/?", &foundSeparator);
		StringRef resource;
		if (foundSeparator == '/') {
			resource = t.eat("?");
		}

		// hostPort is at least a host or IP address, optionally followed by :portNumber or :serviceName
		StringRef h(hostPort);
		StringRef host = h.eat(":");
		if (host.size() == 0)
			throw std::string("host cannot be empty");

		StringRef service = h.eat();

		BlobKnobs knobs;
		while (1) {
			StringRef name = t.eat("=");
			if (name.size() == 0)
				break;
			StringRef value = t.eat("&");

			// See if the parameter is a knob
			bool known = knobs.set(name, 0);

			// If the parameter is not known to GCSBlobStoreEndpoint then throw unless there is an ignored_parameters set
			// to add it to
			if (!known) {
				if (ignored_parameters == nullptr) {
					throw format("%s is not a valid parameter name", name.toString().c_str());
				}
				(*ignored_parameters)[name.toString()] = value.toString();
				continue;
			}

			// The parameter is known to GCSBlobStoreEndpoint so it must be numeric and valid.
			char* valueEnd = nullptr;
			std::string s = value.toString();
			long int ivalue = strtol(s.c_str(), &valueEnd, 10);
			if (*valueEnd || (ivalue == 0 && s != "0") ||
			    (((ivalue == LONG_MAX) || (ivalue == LONG_MIN)) && errno == ERANGE))
				throw format("%s is not a valid value for %s", s.c_str(), name.toString().c_str());

			// It should not be possible for this set to fail now since the dummy set above had to have worked.
			ASSERT(knobs.set(name, ivalue));
		}

		if (resourceFromURL != nullptr)
			*resourceFromURL = resource.toString();

		// Load credentials from environment variable if available
		Credentials creds;
		const char* credsPath = getenv("GCS_CREDENTIALS_FILE");
		if (credsPath) {
			std::string credsFile = credsPath;
			std::ifstream checkFile(credsFile);
			if (checkFile.good()) {
				creds = loadCredentialsFromFile(credsFile);
			}
		}

		return makeReference<GCSBlobStoreEndpoint>(host.toString(), service.toString(), creds, knobs);

	} catch (std::string& err) {
		if (error != nullptr)
			*error = err;
		TraceEvent(SevWarnAlways, "GCSBlobStoreEndpointBadURL")
		    .suppressFor(60)
		    .detail("Description", err)
		    .detail("Format", getURLFormat())
		    .detail("URL", url);
		throw backup_invalid_url();
	}
}

std::string GCSBlobStoreEndpoint::getResourceURL(std::string resource, std::string params) const {
	std::string hostPort = host;
	if (!service.empty()) {
		hostPort.append(":");
		hostPort.append(service);
	}

	std::string r = format("gcs://%s/%s", hostPort.c_str(), resource.c_str());

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

ACTOR Future<GCSBlobStoreEndpoint::ReusableConnection> connect_impl(Reference<GCSBlobStoreEndpoint> b,
                                                                   bool* reusingConn) {
	*reusingConn = false;

	// Try to get a connection from the pool
	while (!b->connectionPool->pool.empty()) {
		GCSBlobStoreEndpoint::ReusableConnection rconn = b->connectionPool->pool.front();
		b->connectionPool->pool.pop();

		if (rconn.expirationTime > now()) {
			*reusingConn = true;
			++b->blobStats->reusedConnections;
			TraceEvent("GCSBlobStoreEndpointReusingConnection")
			    .suppressFor(60)
			    .detail("RemoteEndpoint", rconn.conn->getPeerAddress())
			    .detail("ExpiresIn", rconn.expirationTime - now());
			return rconn;
		}
		++b->blobStats->expiredConnections;
	}

	++b->blobStats->newConnections;
	state std::string host = b->host;
	state std::string service = b->service;

	if (service.empty()) {
		service = b->knobs.secure_connection ? "https" : "http";
	}

	bool isTLS = b->knobs.isTLS();
	state Reference<IConnection> conn;

	wait(store(conn, INetworkConnections::net()->connect(host, service, isTLS)));
	wait(conn->connectHandshake());

	TraceEvent("GCSBlobStoreEndpointNewConnection")
	    .suppressFor(60)
	    .detail("RemoteEndpoint", conn->getPeerAddress())
	    .detail("ExpiresIn", b->knobs.max_connection_life);

	return GCSBlobStoreEndpoint::ReusableConnection({ conn, now() + b->knobs.max_connection_life });
}

Future<GCSBlobStoreEndpoint::ReusableConnection> GCSBlobStoreEndpoint::connect(bool* reusing) {
	return connect_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), reusing);
}

void GCSBlobStoreEndpoint::returnConnection(ReusableConnection& rconn) {
	if (rconn.expirationTime > now()) {
		connectionPool->pool.push(rconn);
	} else {
		++blobStats->expiredConnections;
	}
	rconn.conn = Reference<IConnection>();
}

ACTOR Future<Reference<HTTP::IncomingResponse>> doRequest_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                                               std::string verb,
                                                               std::string resource,
                                                               HTTP::Headers headers,
                                                               UnsentPacketQueue* pContent,
                                                               int contentLen,
                                                               std::set<unsigned int> successCodes) {
	state UnsentPacketQueue contentCopy;
	state Reference<HTTP::OutgoingRequest> req = makeReference<HTTP::OutgoingRequest>();
	req->verb = verb;
	req->data.content = &contentCopy;
	req->data.contentLen = contentLen;
	req->data.headers = headers;
	req->data.headers["Host"] = bstore->host;

	if (!bstore->credentials.isEmpty()) {
		req->data.headers["Authorization"] = "Bearer " + bstore->credentials.token;
	}

	if (resource.empty()) {
		resource = "/";
	}
	req->resource = resource;

	int bandwidthThisRequest = 1 + bstore->knobs.max_send_bytes_per_second / bstore->knobs.concurrent_uploads;
	int contentUploadSeconds = contentLen / bandwidthThisRequest;
	state int requestTimeout = std::max(bstore->knobs.request_timeout_min, 3 * contentUploadSeconds);

	wait(bstore->concurrentRequests.take());
	state FlowLock::Releaser globalReleaser(bstore->concurrentRequests, 1);

	state int maxTries = std::min(bstore->knobs.request_tries, bstore->knobs.connect_tries);
	state int thisTry = 1;
	state double nextRetryDelay = 2.0;

	loop {
		state Optional<Error> err;
		state Reference<HTTP::IncomingResponse> r;
		state bool connectionEstablished = false;
		state UID connID = UID();
		state double reqStartTimer;
		state double connectStartTimer = g_network->timer();
		state bool reusingConn = false;
		state bool fastRetry = false;

		try {
			// Copy content if needed
			req->data.content->discardAll();
			if (pContent != nullptr) {
				PacketBuffer* pFirst = pContent->getUnsent();
				PacketBuffer* pLast = nullptr;
				for (PacketBuffer* p = pFirst; p != nullptr; p = p->nextPacketBuffer()) {
					p->addref();
					p->bytes_sent = 0;
					pLast = p;
				}
				req->data.content->prependWriteBuffer(pFirst, pLast);
			}

			// Connect
			Future<GCSBlobStoreEndpoint::ReusableConnection> frconn = bstore->connect(&reusingConn);
			state GCSBlobStoreEndpoint::ReusableConnection rconn =
			    wait(timeoutError(frconn, bstore->knobs.connect_timeout));
			connectionEstablished = true;
			connID = rconn.conn->getDebugID();
			reqStartTimer = g_network->timer();

			wait(bstore->requestRate->getAllowance(1));

			// Do request
			Future<Reference<HTTP::IncomingResponse>> reqF =
			    HTTP::doRequest(rconn.conn, req, bstore->sendRate, &bstore->s_stats.bytes_sent, bstore->recvRate);

			if (reqF.isReady() && reusingConn) {
				fastRetry = true;
			}

			Reference<HTTP::IncomingResponse> _r = wait(timeoutError(reqF, requestTimeout));
			r = _r;

			if (r->data.headers["Connection"] != "close") {
				bstore->returnConnection(rconn);
			} else {
				++bstore->blobStats->expiredConnections;
			}
			rconn.conn.clear();

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			err = e;
		}

		double end = g_network->timer();
		double connectDuration = reqStartTimer - connectStartTimer;
		double reqDuration = end - reqStartTimer;
		bstore->blobStats->requestLatency.addMeasurement(reqDuration);

		if (!err.present() && successCodes.count(r->code) != 0) {
			bstore->s_stats.requests_successful++;
			++bstore->blobStats->requestsSuccessful;
			return r;
		}

		bstore->s_stats.requests_failed++;
		++bstore->blobStats->requestsFailed;

		bool retryable = err.present() || (r && (r->code == 500 || r->code == 502 || r->code == 503 || r->code == 429));
		retryable = retryable && (thisTry < maxTries);

		if (!retryable || !err.present()) {
			fastRetry = false;
		}

		TraceEvent event(SevWarn,
		                 retryable ? (fastRetry ? "GCSBlobStoreEndpointRequestFailedFastRetryable"
		                                        : "GCSBlobStoreEndpointRequestFailedRetryable")
		                           : "GCSBlobStoreEndpointRequestFailed");

		bool connectionFailed = false;
		if (err.present()) {
			event.errorUnsuppressed(err.get());
			if (err.get().code() == error_code_connection_failed) {
				connectionFailed = true;
			}
		}
		event.suppressFor(60);
		if (!err.present()) {
			event.detail("ResponseCode", r->code);
		}

		event.detail("ConnectionEstablished", connectionEstablished);
		event.detail("ReusingConn", reusingConn);
		if (connectionEstablished) {
			event.detail("ConnID", connID);
			event.detail("ConnectDuration", connectDuration);
			event.detail("ReqDuration", reqDuration);
		}

		event.detail("Verb", verb).detail("Resource", resource).detail("ThisTry", thisTry);

		if (!fastRetry && (!r || r->code != 429))
			++thisTry;

		if (fastRetry) {
			++bstore->blobStats->fastRetries;
			wait(delay(0));
		} else if (retryable) {
			double delay = nextRetryDelay;
			double limit =
			    connectionFailed ? bstore->knobs.max_delay_connection_failed : bstore->knobs.max_delay_retryable_error;
			nextRetryDelay = std::min(nextRetryDelay * 2, limit);

			if (r) {
				auto iRetryAfter = r->data.headers.find("Retry-After");
				if (iRetryAfter != r->data.headers.end()) {
					event.detail("RetryAfterHeader", iRetryAfter->second);
					char* pEnd;
					double retryAfter = strtod(iRetryAfter->second.c_str(), &pEnd);
					if (*pEnd)
						retryAfter = 300;
					delay = std::max(delay, retryAfter);
				}
			}

			event.detail("RetryDelay", delay);
			wait(::delay(delay));
		} else {
			if (r && r->code == 404)
				throw file_not_found();
			if (r && r->code == 401)
				throw http_auth_failed();
			if (err.present()) {
				int code = err.get().code();
				if (code == error_code_timed_out && !connectionEstablished) {
					throw connection_failed();
				}
				if (code == error_code_timed_out || code == error_code_connection_failed ||
				    code == error_code_lookup_failed)
					throw err.get();
			}
			throw http_request_failed();
		}
	}
}

Future<Reference<HTTP::IncomingResponse>> GCSBlobStoreEndpoint::doRequest(std::string const& verb,
                                                                          std::string const& resource,
                                                                          const HTTP::Headers& headers,
                                                                          UnsentPacketQueue* pContent,
                                                                          int contentLen,
                                                                          std::set<unsigned int> successCodes) {
	return doRequest_impl(
	    Reference<GCSBlobStoreEndpoint>::addRef(this), verb, resource, headers, pContent, contentLen, successCodes);
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

	std::string resource = format("/storage/v1/b/%s/o/%s?alt=media", bucket.c_str(), HTTP::awsV4URIEncode(object, true).c_str());

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
	state int64_t size = wait(bstore->objectSize(bucket, object));

	state std::string result;
	result.resize(size);

	int bytesRead = wait(bstore->readObject(bucket, object, &result[0], size, 0));

	if (bytesRead != size)
		throw io_error();

	return result;
}

Future<std::string> GCSBlobStoreEndpoint::readEntireFile(std::string const& bucket, std::string const& object) {
	return readEntireFile_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<bool> bucketExists_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket) {
	std::string resource = format("/storage/v1/b/%s", bucket.c_str());

	try {
		Reference<HTTP::IncomingResponse> r =
		    wait(bstore->doRequest("GET", resource, {}, nullptr, 0, { 200, 404 }));
		return r->code == 200;
	} catch (Error& e) {
		if (e.code() == error_code_file_not_found)
			return false;
		throw;
	}
}

Future<bool> GCSBlobStoreEndpoint::bucketExists(std::string const& bucket) {
	return bucketExists_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<Void> createBucket_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket) {
	// First check if bucket already exists
	bool exists = wait(bstore->bucketExists(bucket));
	if (exists) {
		TraceEvent(SevInfo, "GCSBucketAlreadyExists").detail("Bucket", bucket);
		return Void();
	}

	// For real GCP, we would need a project ID here, but bucket creation is typically
	// done out-of-band. For now, just verify the bucket exists.
	TraceEvent(SevWarn, "GCSBucketCreateSkipped")
	    .detail("Bucket", bucket)
	    .detail("Reason", "Bucket creation requires GCP project ID");

	return Void();
}

Future<Void> GCSBlobStoreEndpoint::createBucket(std::string const& bucket) {
	return createBucket_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<std::vector<std::string>> listBuckets_impl(Reference<GCSBlobStoreEndpoint> bstore) {
	std::string resource = "/storage/v1/b";

	Reference<HTTP::IncomingResponse> r = wait(bstore->doRequest("GET", resource, {}, nullptr, 0, { 200 }));

	std::string response(r->data.content.begin(), r->data.content.end());

	json_spirit::mValue json;
	json_spirit::read_string(response, json);

	if (json.type() != json_spirit::obj_type)
		throw http_bad_response();

	json_spirit::mObject obj = json.get_obj();
	std::vector<std::string> buckets;

	auto itemsIt = obj.find("items");
	if (itemsIt != obj.end() && itemsIt->second.type() == json_spirit::array_type) {
		json_spirit::mArray items = itemsIt->second.get_array();
		for (const auto& item : items) {
			if (item.type() != json_spirit::obj_type)
				continue;

			json_spirit::mObject itemObj = item.get_obj();
			auto nameIt = itemObj.find("name");
			if (nameIt != itemObj.end() && nameIt->second.type() == json_spirit::str_type) {
				buckets.push_back(nameIt->second.get_str());
			}
		}
	}

	return buckets;
}

Future<std::vector<std::string>> GCSBlobStoreEndpoint::listBuckets() {
	return listBuckets_impl(Reference<GCSBlobStoreEndpoint>::addRef(this));
}

ACTOR Future<bool> objectExists_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	std::string resource = format("/storage/v1/b/%s/o/%s", bucket.c_str(), HTTP::awsV4URIEncode(object, true).c_str());

	try {
		Reference<HTTP::IncomingResponse> r =
		    wait(bstore->doRequest("GET", resource, {}, nullptr, 0, { 200, 404 }));
		return r->code == 200;
	} catch (Error& e) {
		if (e.code() == error_code_file_not_found)
			return false;
		throw;
	}
}

Future<bool> GCSBlobStoreEndpoint::objectExists(std::string const& bucket, std::string const& object) {
	return objectExists_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<int64_t> objectSize_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	std::string resource = format("/storage/v1/b/%s/o/%s?fields=size", bucket.c_str(), HTTP::awsV4URIEncode(object, true).c_str());

	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("GET", resource, {}, nullptr, 0, { 200, 404 }));

	if (r->code == 404)
		throw file_not_found();

	std::string response(r->data.content.begin(), r->data.content.end());

	json_spirit::mValue json;
	json_spirit::read_string(response, json);

	if (json.type() != json_spirit::obj_type)
		throw io_error();

	json_spirit::mObject obj = json.get_obj();
	auto sizeIt = obj.find("size");
	if (sizeIt == obj.end())
		throw io_error();

	if (sizeIt->second.type() == json_spirit::str_type)
		return std::stoll(sizeIt->second.get_str());
	else if (sizeIt->second.type() == json_spirit::int_type)
		return sizeIt->second.get_int64();

	throw io_error();
}

Future<int64_t> GCSBlobStoreEndpoint::objectSize(std::string const& bucket, std::string const& object) {
	return objectSize_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteObject_impl(Reference<GCSBlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	wait(bstore->requestRateDelete->getAllowance(1));

	std::string resource = format("/storage/v1/b/%s/o/%s", bucket.c_str(), HTTP::awsV4URIEncode(object, true).c_str());

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

	state std::vector<Future<Void>> deleteFutures;

	try {
		loop {
			GCSBlobStoreEndpoint::ListResult res = waitNext(resultStream.getFuture());

			for (auto& object : res.objects) {
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
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}

	wait(done);
	wait(waitForAll(deleteFutures));

	return Void();
}

Future<Void> GCSBlobStoreEndpoint::deleteRecursively(std::string const& bucket,
                                                      std::string prefix,
                                                      int* pNumDeleted,
                                                      int64_t* pBytesDeleted) {
	return deleteRecursively_impl(
	    Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, prefix, pNumDeleted, pBytesDeleted);
}

ACTOR Future<Void> writeEntireFileFromBuffer_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                                  std::string bucket,
                                                  std::string object,
                                                  UnsentPacketQueue* pContent,
                                                  int contentLen,
                                                  std::string contentMD5) {
	wait(bstore->requestRateWrite->getAllowance(1));
	wait(bstore->concurrentUploads.take());
	state FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = format("/upload/storage/v1/b/%s/o?uploadType=media&name=%s", bucket.c_str(), HTTP::awsV4URIEncode(object, true).c_str());

	HTTP::Headers headers;
	headers["Content-Type"] = "application/octet-stream";
	headers["Content-Length"] = std::to_string(contentLen);
	if (!contentMD5.empty()) {
		headers["Content-MD5"] = contentMD5;
	}

	Reference<HTTP::IncomingResponse> r = wait(
	    bstore->doRequest("POST", resource, headers, pContent, contentLen, { 200, 201 }));

	if (r->code != 200 && r->code != 201)
		throw http_request_failed();

	if (!contentMD5.empty() && !HTTP::verifyMD5(&r->data, false, contentMD5))
		throw checksum_failed();

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

	std::string resource = format("/upload/storage/v1/b/%s/o?uploadType=media&name=%s", bucket.c_str(), HTTP::awsV4URIEncode(object, true).c_str());

	PacketWriter writer(packets.getWriteBuffer(content.size()), nullptr, Unversioned());
	writer.serializeBytes(content);

	HTTP::Headers headers;
	headers["Content-Type"] = "application/octet-stream";
	headers["Content-Length"] = std::to_string(content.size());

	Reference<HTTP::IncomingResponse> r = wait(
	    bstore->doRequest("POST", resource, headers, &packets, content.size(), { 200, 201 }));

	if (r->code != 200 && r->code != 201)
		throw http_request_failed();

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
	std::string resource = format("/upload/storage/v1/b/%s/o?uploadType=resumable&name=%s", bucket.c_str(), HTTP::awsV4URIEncode(object, true).c_str());

	HTTP::Headers headers;
	headers["Content-Type"] = "application/octet-stream";
	headers["Content-Length"] = "0";

	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("POST", resource, headers, nullptr, 0, { 200 }));

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

ACTOR Future<std::string> uploadPart_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                          std::string bucket,
                                          std::string object,
                                          std::string uploadID,
                                          unsigned int partNumber,
                                          UnsentPacketQueue* pContent,
                                          int contentLen,
                                          std::string contentMD5) {
	std::string resource = format("/upload/storage/v1/b/%s/o?uploadType=resumable&upload_id=%s", bucket.c_str(), uploadID.c_str());

	int64_t rangeStart = (partNumber - 1) * contentLen;
	int64_t rangeEnd = rangeStart + contentLen - 1;

	HTTP::Headers headers;
	headers["Content-Type"] = "application/octet-stream";
	headers["Content-Length"] = std::to_string(contentLen);
	headers["Content-Range"] = format("bytes %lld-%lld/*", rangeStart, rangeEnd);
	if (!contentMD5.empty()) {
		headers["Content-MD5"] = contentMD5;
	}

	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, { 200, 308 }));

	if (!contentMD5.empty() && !HTTP::verifyMD5(&r->data, false, contentMD5))
		throw checksum_failed();

	return format("part-%d", partNumber);
}

Future<std::string> GCSBlobStoreEndpoint::uploadPart(std::string const& bucket,
                                                      std::string const& object,
                                                      std::string const& uploadID,
                                                      unsigned int partNumber,
                                                      UnsentPacketQueue* pContent,
                                                      int contentLen,
                                                      std::string const& contentMD5) {
	return uploadPart_impl(
	    Reference<GCSBlobStoreEndpoint>::addRef(this), bucket, object, uploadID, partNumber, pContent, contentLen, contentMD5);
}

ACTOR Future<Void> finishMultiPartUpload_impl(Reference<GCSBlobStoreEndpoint> bstore,
                                               std::string bucket,
                                               std::string object,
                                               std::string uploadID,
                                               GCSBlobStoreEndpoint::MultiPartSetT parts) {
	state int totalParts = parts.size();
	if (totalParts == 0)
		throw http_bad_response();

	int lastPartNum = parts.rbegin()->first;
	int contentLen = 0;
	int64_t totalSize = lastPartNum * contentLen;

	std::string resource = format("/upload/storage/v1/b/%s/o?uploadType=resumable&upload_id=%s", bucket.c_str(), uploadID.c_str());

	HTTP::Headers headers;
	headers["Content-Length"] = "0";
	headers["Content-Range"] = format("bytes */%lld", totalSize);

	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("PUT", resource, headers, nullptr, 0, { 200, 201 }));

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
	wait(bstore->requestRateList->getAllowance(1));

	state std::string resource = format("/storage/v1/b/%s/o?maxResults=1000", bucket.c_str());
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

		Reference<HTTP::IncomingResponse> r =
		    wait(bstore->doRequest("GET", fullResource, {}, nullptr, 0, { 200 }));
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
					continue;

				json_spirit::mObject itemObj = item.get_obj();
				GCSBlobStoreEndpoint::ObjectInfo objInfo;

				auto nameIt = itemObj.find("name");
				if (nameIt != itemObj.end() && nameIt->second.type() == json_spirit::str_type)
					objInfo.name = nameIt->second.get_str();

				auto sizeIt = itemObj.find("size");
				if (sizeIt != itemObj.end()) {
					if (sizeIt->second.type() == json_spirit::str_type)
						objInfo.size = std::stoll(sizeIt->second.get_str());
					else if (sizeIt->second.type() == json_spirit::int_type)
						objInfo.size = sizeIt->second.get_int64();
				}

				listResult.objects.push_back(objInfo);
			}
		}

		auto prefixesIt = obj.find("prefixes");
		if (prefixesIt != obj.end() && prefixesIt->second.type() == json_spirit::array_type) {
			json_spirit::mArray prefixes = prefixesIt->second.get_array();
			for (const auto& prefixVal : prefixes) {
				if (prefixVal.type() != json_spirit::str_type)
					continue;

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

		auto nextPageIt = obj.find("nextPageToken");
		if (nextPageIt != obj.end() && nextPageIt->second.type() == json_spirit::str_type) {
			pageToken = nextPageIt->second.get_str();
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
			GCSBlobStoreEndpoint::ListResult res = waitNext(resultStream.getFuture());
			results.commonPrefixes.insert(
			    results.commonPrefixes.end(), res.commonPrefixes.begin(), res.commonPrefixes.end());
			results.objects.insert(results.objects.end(), res.objects.begin(), res.objects.end());
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

GCSBlobStoreEndpoint::Credentials GCSBlobStoreEndpoint::loadCredentialsFromFile(std::string const& filename) {
	try {
		std::ifstream file(filename);
		if (!file.is_open())
			throw io_error();

		std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

		json_spirit::mValue json;
		json_spirit::read_string(content, json);

		if (json.type() != json_spirit::obj_type)
			throw http_bad_response();

		json_spirit::mObject obj = json.get_obj();
		Credentials creds;

		auto tokenIt = obj.find("token");
		if (tokenIt != obj.end() && tokenIt->second.type() == json_spirit::str_type)
			creds.token = tokenIt->second.get_str();

		return creds;
	} catch (Error& e) {
		TraceEvent(SevWarn, "GCSCredentialLoadFailed").error(e).detail("File", filename);
		throw;
	} catch (std::exception& e) {
		TraceEvent(SevWarn, "GCSCredentialLoadFailed").detail("File", filename).detail("Error", e.what());
		throw io_error();
	}
}

// Test configuration helpers
namespace {
	struct GCSTestConfig {
		std::string host;
		std::string service;
		std::string bucket;
		GCSBlobStoreEndpoint::Credentials credentials;
		BlobKnobs knobs;

		static GCSTestConfig getConfig() {
			GCSTestConfig config;

			// Check for GCP credentials file via environment variable or default path
			const char* credsPath = getenv("GCS_CREDENTIALS_FILE");
			std::string credsFile = credsPath ? credsPath : "";
		    TraceEvent("GCSTestConfig").detail("credsPath", credsFile);

			if (!credsFile.empty()) {
				std::ifstream checkFile(credsFile);
				if (checkFile.good()) {
					// Use real GCP
					config.host = "storage.googleapis.com";
					config.service = "443";
					config.bucket = "palantir-foundationdb-gcp-dev";
					config.credentials = GCSBlobStoreEndpoint::loadCredentialsFromFile(credsFile);
					config.knobs.secure_connection = 1;
					TraceEvent("GCSTestConfig").detail("Mode", "RealGCP").detail("Bucket", config.bucket);
					return config;
				}
			}
		    // Use local emulator
		    config.host = "localhost";
		    config.service = "9023";
		    config.bucket = "test-bucket";
		    config.knobs.secure_connection = 0;
		    TraceEvent("GCSTestConfig").detail("Mode", "LocalEmulator").detail("Bucket", config.bucket);
			return config;
		}
	};

	Reference<GCSBlobStoreEndpoint> makeTestEndpoint() {
		GCSTestConfig config = GCSTestConfig::getConfig();
		return makeReference<GCSBlobStoreEndpoint>(config.host, config.service, config.credentials, config.knobs);
	}

	std::string getTestBucket() {
		return GCSTestConfig::getConfig().bucket;
	}
} // namespace

TEST_CASE("/fdbclient/gcsblobstore/readobject") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::vector<uint8_t> buffer;
	buffer.resize(1024);

	TraceEvent("GCSBlobStoreTest_Starting")
		.detail("Host", gcs->host)
		.detail("Service", gcs->service);

	state std::string object = "blob1";

	state int bytesRead = wait(gcs->readObject(bucket, object, buffer.data(), buffer.size(), 0));

	TraceEvent("GCSBlobStoreTest_ReadSuccess")
		.detail("Bucket", bucket)
		.detail("Object", object)
		.detail("BytesRead", bytesRead);

	ASSERT(bytesRead > 0);

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/objectexists") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string object = "blob1";

	state bool exists = wait(gcs->objectExists(bucket, object));

	TraceEvent("GCSBlobStoreTest_ObjectExists")
	    .detail("Bucket", bucket)
	    .detail("Object", object)
	    .detail("Exists", exists);

	ASSERT(exists);

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/objectsize") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string object = "blob1";

	state int64_t size = wait(gcs->objectSize(bucket, object));

	TraceEvent("GCSBlobStoreTest_ObjectSize")
	    .detail("Bucket", bucket)
	    .detail("Object", object)
	    .detail("Size", size);

	ASSERT(size > 0);

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/writeandread") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string object = "test-write-" + deterministicRandom()->randomUniqueID().shortString();
	state std::string content = "Hello from FoundationDB GCS test!";

	TraceEvent("GCSBlobStoreTest_WriteStart").detail("Bucket", bucket).detail("Object", object);

	wait(gcs->writeEntireFile(bucket, object, content));

	TraceEvent("GCSBlobStoreTest_WriteComplete").detail("Object", object);

	state std::vector<uint8_t> buffer;
	buffer.resize(1024);

	state int bytesRead = wait(gcs->readObject(bucket, object, buffer.data(), buffer.size(), 0));

	TraceEvent("GCSBlobStoreTest_ReadComplete").detail("Object", object).detail("BytesRead", bytesRead);

	ASSERT(bytesRead == content.size());
	ASSERT(memcmp(buffer.data(), content.data(), bytesRead) == 0);

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/integrationtest") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string object = "integration-test-" + deterministicRandom()->randomUniqueID().shortString();
	state std::string content = "Integration test content for GCS BlobStore implementation";

	TraceEvent("GCSIntegrationTest_Start").detail("Bucket", bucket).detail("Object", object);

	// Step 1: Write file
	TraceEvent("GCSIntegrationTest_Writing").detail("ContentSize", content.size());
	wait(gcs->writeEntireFile(bucket, object, content));

	// Step 2: Verify it exists
	state bool exists = wait(gcs->objectExists(bucket, object));
	TraceEvent("GCSIntegrationTest_ExistsCheck").detail("Exists", exists);
	ASSERT(exists);

	// Step 3: Verify size is reported accurately
	state int64_t size = wait(gcs->objectSize(bucket, object));
	TraceEvent("GCSIntegrationTest_SizeCheck").detail("ReportedSize", size).detail("ExpectedSize", content.size());
	ASSERT(size == content.size());

	// Step 4: Read file and verify contents
	state std::vector<uint8_t> buffer;
	buffer.resize(content.size() + 10);
	state int bytesRead = wait(gcs->readObject(bucket, object, buffer.data(), buffer.size(), 0));
	TraceEvent("GCSIntegrationTest_ReadCheck").detail("BytesRead", bytesRead);
	ASSERT(bytesRead == content.size());
	ASSERT(memcmp(buffer.data(), content.data(), bytesRead) == 0);

	// Step 5: Delete the file
	TraceEvent("GCSIntegrationTest_Deleting");
	wait(gcs->deleteObject(bucket, object));

	// Step 6: Verify it no longer exists
	state bool existsAfterDelete = wait(gcs->objectExists(bucket, object));
	TraceEvent("GCSIntegrationTest_ExistsAfterDelete").detail("Exists", existsAfterDelete);
	ASSERT(!existsAfterDelete);

	TraceEvent("GCSIntegrationTest_Success");

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/writefrombuffer") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string object = "written-from-fdb-cpp-" + deterministicRandom()->randomUniqueID().shortString();
	state std::string content = "Testing writeEntireFileFromBuffer with MD5 checksum verification";

	TraceEvent("GCSBufferTest_Start").detail("Bucket", bucket).detail("Object", object);

	UnsentPacketQueue packets;
	PacketWriter writer(packets.getWriteBuffer(content.size()), nullptr, Unversioned());
	writer.serializeBytes(content);

	state std::string contentMD5 = HTTP::computeMD5Sum(content);

	TraceEvent("GCSBufferTest_Writing")
	    .detail("ContentSize", content.size())
	    .detail("MD5", contentMD5);

	wait(gcs->writeEntireFileFromBuffer(bucket, object, &packets, content.size(), contentMD5));

	state std::vector<uint8_t> buffer;
	buffer.resize(content.size() + 10);
	state int bytesRead = wait(gcs->readObject(bucket, object, buffer.data(), buffer.size(), 0));

	TraceEvent("GCSBufferTest_Verification")
	    .detail("BytesRead", bytesRead)
	    .detail("ExpectedSize", content.size());

	ASSERT(bytesRead == content.size());
	ASSERT(memcmp(buffer.data(), content.data(), bytesRead) == 0);

	// wait(gcs->deleteObject(bucket, object));

	TraceEvent("GCSBufferTest_Success");

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/multipart") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string object = "multipart-" + deterministicRandom()->randomUniqueID().shortString();

	state std::string part1Content = "Part 1: The quick brown fox ";
	state std::string part2Content = "Part 2: jumps over the lazy dog";
	state std::string expectedContent = part1Content + part2Content;

	TraceEvent("GCSMultipartTest_Start").detail("Bucket", bucket).detail("Object", object);

	state std::string uploadID = wait(gcs->beginMultiPartUpload(bucket, object));
	TraceEvent("GCSMultipartTest_UploadStarted").detail("UploadID", uploadID);

	UnsentPacketQueue packets1;
	PacketWriter writer1(packets1.getWriteBuffer(part1Content.size()), nullptr, Unversioned());
	writer1.serializeBytes(part1Content);
	state std::string md5_1 = HTTP::computeMD5Sum(part1Content);

	state std::string etag1 = wait(gcs->uploadPart(bucket, object, uploadID, 1, &packets1, part1Content.size(), md5_1));
	TraceEvent("GCSMultipartTest_Part1Uploaded").detail("ETag", etag1);

	UnsentPacketQueue packets2;
	PacketWriter writer2(packets2.getWriteBuffer(part2Content.size()), nullptr, Unversioned());
	writer2.serializeBytes(part2Content);
	state std::string md5_2 = HTTP::computeMD5Sum(part2Content);

	state std::string etag2 = wait(gcs->uploadPart(bucket, object, uploadID, 2, &packets2, part2Content.size(), md5_2));
	TraceEvent("GCSMultipartTest_Part2Uploaded").detail("ETag", etag2);

	state GCSBlobStoreEndpoint::MultiPartSetT parts;
	parts[1] = etag1;
	parts[2] = etag2;

	wait(gcs->finishMultiPartUpload(bucket, object, uploadID, parts));
	TraceEvent("GCSMultipartTest_Finalized");

	state bool exists = wait(gcs->objectExists(bucket, object));
	ASSERT(exists);

	state std::vector<uint8_t> buffer;
	buffer.resize(expectedContent.size() + 10);
	state int bytesRead = wait(gcs->readObject(bucket, object, buffer.data(), buffer.size(), 0));

	TraceEvent("GCSMultipartTest_Verification")
	    .detail("BytesRead", bytesRead)
	    .detail("ExpectedSize", expectedContent.size());

	ASSERT(bytesRead == expectedContent.size());
	ASSERT(memcmp(buffer.data(), expectedContent.data(), bytesRead) == 0);

	wait(gcs->deleteObject(bucket, object));

	TraceEvent("GCSMultipartTest_Success");

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/listobjects") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string prefix = "list-test-";
	state std::string uniqueId = deterministicRandom()->randomUniqueID().shortString();

	TraceEvent("GCSListTest_Start").detail("Bucket", bucket).detail("Prefix", prefix);

	state std::vector<std::string> testObjects;
	testObjects.push_back(prefix + uniqueId + "-file1.txt");
	testObjects.push_back(prefix + uniqueId + "-file2.txt");
	testObjects.push_back(prefix + uniqueId + "-subdir/file3.txt");

	state int i;
	for (i = 0; i < testObjects.size(); i++) {
		std::string content = format("Content for %s", testObjects[i].c_str());
		wait(gcs->writeEntireFile(bucket, testObjects[i], content));
		TraceEvent("GCSListTest_ObjectCreated").detail("Object", testObjects[i]);
	}

	state GCSBlobStoreEndpoint::ListResult result =
	    wait(gcs->listObjects(bucket, prefix + uniqueId, Optional<char>(), 0, nullptr));

	TraceEvent("GCSListTest_Listed")
	    .detail("ObjectCount", result.objects.size())
	    .detail("PrefixCount", result.commonPrefixes.size());

	ASSERT(result.objects.size() >= 2);

	bool found1 = false, found2 = false;
	for (const auto& obj : result.objects) {
		TraceEvent("GCSListTest_FoundObject").detail("Name", obj.name).detail("Size", obj.size);
		if (obj.name == testObjects[0])
			found1 = true;
		if (obj.name == testObjects[1])
			found2 = true;
	}
	ASSERT(found1 && found2);

	for (i = 0; i < testObjects.size(); i++) {
		wait(gcs->deleteObject(bucket, testObjects[i]));
	}

	TraceEvent("GCSListTest_Success");

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/readentirefile") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string object = "readentire-" + deterministicRandom()->randomUniqueID().shortString();
	state std::string content = "This is a test file for readEntireFile method";

	TraceEvent("GCSReadEntireFileTest_Start").detail("Bucket", bucket).detail("Object", object);

	wait(gcs->writeEntireFile(bucket, object, content));

	state std::string readContent = wait(gcs->readEntireFile(bucket, object));

	TraceEvent("GCSReadEntireFileTest_Verification")
	    .detail("ExpectedSize", content.size())
	    .detail("ActualSize", readContent.size());

	ASSERT(readContent == content);

	wait(gcs->deleteObject(bucket, object));

	TraceEvent("GCSReadEntireFileTest_Success");

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/createbucket") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = "test-bucket-" + deterministicRandom()->randomUniqueID().shortString();

	TraceEvent("GCSCreateBucketTest_Start").detail("Bucket", bucket);

	wait(gcs->createBucket(bucket));

	state bool exists = wait(gcs->bucketExists(bucket));

	TraceEvent("GCSCreateBucketTest_Verification").detail("Exists", exists);

	ASSERT(exists);

	TraceEvent("GCSCreateBucketTest_Success");

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/listbuckets") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();

	TraceEvent("GCSListBucketsTest_Start");

	state std::vector<std::string> buckets = wait(gcs->listBuckets());

	TraceEvent("GCSListBucketsTest_Listed").detail("BucketCount", buckets.size());

	for (const auto& bucket : buckets) {
		TraceEvent("GCSListBucketsTest_FoundBucket").detail("Name", bucket);
	}

	ASSERT(buckets.size() > 0);

	TraceEvent("GCSListBucketsTest_Success");

	return Void();
}

TEST_CASE("/fdbclient/gcsblobstore/deleterecursively") {
	state Reference<GCSBlobStoreEndpoint> gcs = makeTestEndpoint();
	state std::string bucket = getTestBucket();
	state std::string prefix = "delete-recursive-" + deterministicRandom()->randomUniqueID().shortString() + "/";

	TraceEvent("GCSDeleteRecursivelyTest_Start").detail("Bucket", bucket).detail("Prefix", prefix);

	state std::vector<std::string> testObjects;
	testObjects.push_back(prefix + "file1.txt");
	testObjects.push_back(prefix + "file2.txt");
	testObjects.push_back(prefix + "subdir/file3.txt");
	testObjects.push_back(prefix + "subdir/file4.txt");

	state int i;
	for (i = 0; i < testObjects.size(); i++) {
		std::string content = format("Content for %s", testObjects[i].c_str());
		wait(gcs->writeEntireFile(bucket, testObjects[i], content));
		TraceEvent("GCSDeleteRecursivelyTest_ObjectCreated").detail("Object", testObjects[i]);
	}

	state int numDeleted = 0;
	state int64_t bytesDeleted = 0;

	TraceEvent("GCSDeleteRecursivelyTest_Deleting").detail("Prefix", prefix);
	wait(gcs->deleteRecursively(bucket, prefix, &numDeleted, &bytesDeleted));

	TraceEvent("GCSDeleteRecursivelyTest_DeleteComplete")
	    .detail("NumDeleted", numDeleted)
	    .detail("BytesDeleted", bytesDeleted);

	ASSERT(numDeleted == testObjects.size());
	ASSERT(bytesDeleted > 0);

	for (i = 0; i < testObjects.size(); i++) {
		state bool exists = wait(gcs->objectExists(bucket, testObjects[i]));
		TraceEvent("GCSDeleteRecursivelyTest_VerifyDeleted")
		    .detail("Object", testObjects[i])
		    .detail("Exists", exists);
		ASSERT(!exists);
	}

	TraceEvent("GCSDeleteRecursivelyTest_Success");

	return Void();
}
