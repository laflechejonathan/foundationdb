//
// Created by Jonathan Lafleche on 5/11/25.
//
#include "boost/algorithm/string/join.hpp"
#include "fdbclient/IBlobStoreEndpoint.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/S3BlobStore.h"
#include "fdbrpc/HTTP.h"
#include "flow/Hostname.h"
#include "flow/IAsyncFile.h"
#include "flow/IConnection.h"
#include "flow/actorcompiler.h" // has to be last include

json_spirit::mObject IBlobStoreEndpoint::Stats::getJSON() {
	json_spirit::mObject o;

	o["requests_failed"] = requests_failed;
	o["requests_successful"] = requests_successful;
	o["bytes_sent"] = bytes_sent;

	return o;
}

IBlobStoreEndpoint::Stats IBlobStoreEndpoint::Stats::operator-(const Stats& rhs) {
	Stats r;
	r.requests_failed = requests_failed - rhs.requests_failed;
	r.requests_successful = requests_successful - rhs.requests_successful;
	r.bytes_sent = bytes_sent - rhs.bytes_sent;
	return r;
}

IBlobStoreEndpoint::Stats IBlobStoreEndpoint::s_stats;
std::unique_ptr<IBlobStoreEndpoint::BlobStats> IBlobStoreEndpoint::blobStats;
Future<Void> IBlobStoreEndpoint::statsLogger = Never();

// Try to read a file, parse it as JSON, and return the resulting document.
// It will NOT throw if any errors are encountered, it will just return an empty
// JSON object and will log trace events for the errors encountered.
ACTOR Future<Optional<json_spirit::mObject>> tryReadJSONFile_impl(std::string path) {
	state std::string content;

	// Event type to be logged in the event of an exception
	state const char* errorEventType = "BlobCredentialFileError";

	try {
		state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
		    path, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0));
		state int64_t size = wait(f->size());
		state Standalone<StringRef> buf = makeString(size);
		int r = wait(f->read(mutateString(buf), size, 0));
		ASSERT(r == size);
		content = buf.toString();

		// Any exceptions from here forward are parse failures
		errorEventType = "BlobCredentialFileParseFailed";
		json_spirit::mValue json;
		json_spirit::read_string(content, json);
		if (json.type() == json_spirit::obj_type)
			return json.get_obj();
		else
			TraceEvent(SevWarn, "BlobCredentialFileNotJSONObject").suppressFor(60).detail("File", path);

	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			TraceEvent(SevWarn, errorEventType).errorUnsuppressed(e).suppressFor(60).detail("File", path);
	}

	return Optional<json_spirit::mObject>();
}

Future<Optional<json_spirit::mObject>> tryReadJSONFile(std::string path) {
	return tryReadJSONFile_impl(path);
}

void parseProxy(Optional<std::string> const& proxy,
                Optional<std::string>& proxyHost,
                Optional<std::string>& proxyPort) {}

std::unordered_map<BlobStoreConnectionPoolKey, Reference<IBlobStoreEndpoint::ConnectionPoolData>>
    IBlobStoreEndpoint::globalConnectionPool;

BlobKnobs::BlobKnobs() {
	secure_connection = 1;
	connect_tries = CLIENT_KNOBS->BLOBSTORE_CONNECT_TRIES;
	connect_timeout = CLIENT_KNOBS->BLOBSTORE_CONNECT_TIMEOUT;
	max_connection_life = CLIENT_KNOBS->BLOBSTORE_MAX_CONNECTION_LIFE;
	request_tries = CLIENT_KNOBS->BLOBSTORE_REQUEST_TRIES;
	request_timeout_min = CLIENT_KNOBS->BLOBSTORE_REQUEST_TIMEOUT_MIN;
	requests_per_second = CLIENT_KNOBS->BLOBSTORE_REQUESTS_PER_SECOND;
	concurrent_requests = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS;
	list_requests_per_second = CLIENT_KNOBS->BLOBSTORE_LIST_REQUESTS_PER_SECOND;
	write_requests_per_second = CLIENT_KNOBS->BLOBSTORE_WRITE_REQUESTS_PER_SECOND;
	read_requests_per_second = CLIENT_KNOBS->BLOBSTORE_READ_REQUESTS_PER_SECOND;
	delete_requests_per_second = CLIENT_KNOBS->BLOBSTORE_DELETE_REQUESTS_PER_SECOND;
	multipart_max_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MAX_PART_SIZE;
	multipart_min_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	concurrent_uploads = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_UPLOADS;
	concurrent_lists = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_LISTS;
	concurrent_reads_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_READS_PER_FILE;
	concurrent_writes_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_WRITES_PER_FILE;
	enable_read_cache = CLIENT_KNOBS->BLOBSTORE_ENABLE_READ_CACHE;
	read_block_size = CLIENT_KNOBS->BLOBSTORE_READ_BLOCK_SIZE;
	read_ahead_blocks = CLIENT_KNOBS->BLOBSTORE_READ_AHEAD_BLOCKS;
	read_cache_blocks_per_file = CLIENT_KNOBS->BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE;
	max_send_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_SEND_BYTES_PER_SECOND;
	max_recv_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_RECV_BYTES_PER_SECOND;
	max_delay_retryable_error = CLIENT_KNOBS->BLOBSTORE_MAX_DELAY_RETRYABLE_ERROR;
	max_delay_connection_failed = CLIENT_KNOBS->BLOBSTORE_MAX_DELAY_CONNECTION_FAILED;
	sdk_auth = false;
	global_connection_pool = CLIENT_KNOBS->BLOBSTORE_GLOBAL_CONNECTION_POOL;
}

bool BlobKnobs::set(StringRef name, int value) {
#define TRY_PARAM(n, sn)                                                                                               \
	if (name == #n || name == #sn) {                                                                                   \
		n = value;                                                                                                     \
		return true;                                                                                                   \
	}
	TRY_PARAM(secure_connection, sc)
	TRY_PARAM(connect_tries, ct);
	TRY_PARAM(connect_timeout, cto);
	TRY_PARAM(max_connection_life, mcl);
	TRY_PARAM(request_tries, rt);
	TRY_PARAM(request_timeout_min, rtom);
	// TODO: For backward compatibility because request_timeout was renamed to request_timeout_min
	if (name == "request_timeout"_sr || name == "rto"_sr) {
		request_timeout_min = value;
		return true;
	}
	TRY_PARAM(requests_per_second, rps);
	TRY_PARAM(list_requests_per_second, lrps);
	TRY_PARAM(write_requests_per_second, wrps);
	TRY_PARAM(read_requests_per_second, rrps);
	TRY_PARAM(delete_requests_per_second, drps);
	TRY_PARAM(concurrent_requests, cr);
	TRY_PARAM(multipart_max_part_size, maxps);
	TRY_PARAM(multipart_min_part_size, minps);
	TRY_PARAM(concurrent_uploads, cu);
	TRY_PARAM(concurrent_lists, cl);
	TRY_PARAM(concurrent_reads_per_file, crpf);
	TRY_PARAM(concurrent_writes_per_file, cwpf);
	TRY_PARAM(enable_read_cache, erc);
	TRY_PARAM(read_block_size, rbs);
	TRY_PARAM(read_ahead_blocks, rab);
	TRY_PARAM(read_cache_blocks_per_file, rcb);
	TRY_PARAM(max_send_bytes_per_second, sbps);
	TRY_PARAM(max_recv_bytes_per_second, rbps);
	TRY_PARAM(max_delay_retryable_error, dre);
	TRY_PARAM(max_delay_connection_failed, dcf);
	TRY_PARAM(sdk_auth, sa);
	TRY_PARAM(global_connection_pool, gcp);
#undef TRY_PARAM
	return false;
}

// Returns a Blob URL parameter string that specifies all of the non-default options for the endpoint using option
// short names.
std::string BlobKnobs::getURLParameters() const {
	static BlobKnobs defaults;
	std::string r;
#define _CHECK_PARAM(n, sn)                                                                                            \
	if (n != defaults.n) {                                                                                             \
		r += format("%s%s=%d", r.empty() ? "" : "&", #sn, n);                                                          \
	}
	_CHECK_PARAM(secure_connection, sc);
	_CHECK_PARAM(connect_tries, ct);
	_CHECK_PARAM(connect_timeout, cto);
	_CHECK_PARAM(max_connection_life, mcl);
	_CHECK_PARAM(request_tries, rt);
	_CHECK_PARAM(request_timeout_min, rto);
	_CHECK_PARAM(requests_per_second, rps);
	_CHECK_PARAM(list_requests_per_second, lrps);
	_CHECK_PARAM(write_requests_per_second, wrps);
	_CHECK_PARAM(read_requests_per_second, rrps);
	_CHECK_PARAM(delete_requests_per_second, drps);
	_CHECK_PARAM(concurrent_requests, cr);
	_CHECK_PARAM(multipart_max_part_size, maxps);
	_CHECK_PARAM(multipart_min_part_size, minps);
	_CHECK_PARAM(concurrent_uploads, cu);
	_CHECK_PARAM(concurrent_lists, cl);
	_CHECK_PARAM(concurrent_reads_per_file, crpf);
	_CHECK_PARAM(concurrent_writes_per_file, cwpf);
	_CHECK_PARAM(enable_read_cache, erc);
	_CHECK_PARAM(read_block_size, rbs);
	_CHECK_PARAM(read_ahead_blocks, rab);
	_CHECK_PARAM(read_cache_blocks_per_file, rcb);
	_CHECK_PARAM(max_send_bytes_per_second, sbps);
	_CHECK_PARAM(max_recv_bytes_per_second, rbps);
	_CHECK_PARAM(sdk_auth, sa);
	_CHECK_PARAM(global_connection_pool, gcp);
	_CHECK_PARAM(max_delay_retryable_error, dre);
	_CHECK_PARAM(max_delay_connection_failed, dcf);
#undef _CHECK_PARAM
	return r;
}

ACTOR Future<IBlobStoreEndpoint::ReusableConnection> connect_impl(Reference<IBlobStoreEndpoint> b, bool* reusingConn) {
	// First try to get a connection from the pool
	*reusingConn = false;
	while (!b->connectionPool->pool.empty()) {
		IBlobStoreEndpoint::ReusableConnection rconn = b->connectionPool->pool.front();
		b->connectionPool->pool.pop();

		// If the connection expires in the future then return it
		if (rconn.expirationTime > now()) {
			*reusingConn = true;
			++b->blobStats->reusedConnections;
			TraceEvent("IBlobStoreEndpointReusingConnected")
			    .suppressFor(60)
			    .detail("RemoteEndpoint", rconn.conn->getPeerAddress())
			    .detail("ExpiresIn", rconn.expirationTime - now())
			    .detail("Proxy", b->proxyHost.orDefault(""));
			return rconn;
		}
		++b->blobStats->expiredConnections;
	}
	++b->blobStats->newConnections;
	std::string host = b->host, service = b->service;
	TraceEvent(SevDebug, "IBlobStoreEndpointBuildingNewConnection")
	    .detail("UseProxy", b->useProxy)
	    .detail("TLS", b->knobs.secure_connection == 1)
	    .detail("Host", host)
	    .detail("Service", service)
	    .log();
	if (service.empty()) {
		if (b->useProxy) {
			fprintf(stderr, "ERROR: Port can't be empty when using HTTP proxy.\n");
			throw connection_failed();
		}
		service = b->knobs.secure_connection ? "https" : "http";
	}
	bool isTLS = b->knobs.isTLS();
	state Reference<IConnection> conn;
	if (b->useProxy) {
		if (isTLS) {
			Reference<IConnection> _conn =
			    wait(HTTP::proxyConnect(host, service, b->proxyHost.get(), b->proxyPort.get()));
			conn = _conn;
		} else {
			host = b->proxyHost.get();
			service = b->proxyPort.get();
			Reference<IConnection> _conn = wait(INetworkConnections::net()->connect(host, service, false));
			conn = _conn;
		}
	} else {
		wait(store(conn, INetworkConnections::net()->connect(host, service, isTLS)));
	}
	wait(conn->connectHandshake());

	TraceEvent("IBlobStoreEndpointNewConnectionSuccess")
	    .suppressFor(60)
	    .detail("RemoteEndpoint", conn->getPeerAddress())
	    .detail("ExpiresIn", b->knobs.max_connection_life)
	    .detail("Proxy", b->proxyHost.orDefault(""));

	if (b->lookupSecretOnEachRequest()) {
		wait(b->updateSecret());
	}

	return IBlobStoreEndpoint::ReusableConnection({ conn, now() + b->knobs.max_connection_life });
}

Future<IBlobStoreEndpoint::ReusableConnection> IBlobStoreEndpoint::connect(bool* reusing) {
	return connect_impl(Reference<IBlobStoreEndpoint>::addRef(this), reusing);
}

// Do a request, get a Response.
// Request content is provided as UnsentPacketQueue *pContent which will be depleted as bytes are sent but the queue
// itself must live for the life of this actor and be destroyed by the caller
ACTOR Future<Reference<HTTP::IncomingResponse>> doRequest_impl(Reference<IBlobStoreEndpoint> bstore,
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

	// Avoid to send request with an empty resouce.
	if (resource.empty()) {
		resource = "/";
	}

	// Merge extraHeaders into headers
	for (const auto& [k, v] : bstore->extraHeaders) {
		std::string& fieldValue = req->data.headers[k];
		if (!fieldValue.empty()) {
			fieldValue.append(",");
		}
		fieldValue.append(v);
	}

	// For requests with content to upload, the request timeout should be at least twice the amount of time
	// it would take to upload the content given the upload bandwidth and concurrency limits.
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
		state Optional<NetworkAddress> remoteAddress;
		state bool connectionEstablished = false;
		state Reference<HTTP::IncomingResponse> r;
		state std::string canonicalURI = resource;
		state UID connID = UID();
		state double reqStartTimer;
		state double connectStartTimer = g_network->timer();
		state bool reusingConn = false;
		state bool fastRetry = false;

		try {
			// Start connecting
			Future<IBlobStoreEndpoint::ReusableConnection> frconn = bstore->connect(&reusingConn);

			// Make a shallow copy of the queue by calling addref() on each buffer in the chain and then prepending that
			// chain to contentCopy
			req->data.content->discardAll();
			if (pContent != nullptr) {
				PacketBuffer* pFirst = pContent->getUnsent();
				PacketBuffer* pLast = nullptr;
				for (PacketBuffer* p = pFirst; p != nullptr; p = p->nextPacketBuffer()) {
					p->addref();
					// Also reset the sent count on each buffer
					p->bytes_sent = 0;
					pLast = p;
				}
				req->data.content->prependWriteBuffer(pFirst, pLast);
			}

			// Finish connecting, do request
			state IBlobStoreEndpoint::ReusableConnection rconn =
			    wait(timeoutError(frconn, bstore->knobs.connect_timeout));
			connectionEstablished = true;
			connID = rconn.conn->getDebugID();
			reqStartTimer = g_network->timer();

			bstore->setAllRequestHeaders(verb, resource, req->data.headers);
			canonicalURI = bstore->normalizeURIForRemoteRequest(resource);

			if (bstore->useProxy && bstore->knobs.secure_connection == 0) {
				// Has to be in absolute-form.
				canonicalURI = "http://" + bstore->host + ":" + bstore->service + canonicalURI;
			}

			req->resource = canonicalURI;

			remoteAddress = rconn.conn->getPeerAddress();
			wait(bstore->requestRate->getAllowance(1));

			Future<Reference<HTTP::IncomingResponse>> reqF =
			    HTTP::doRequest(rconn.conn, req, bstore->sendRate, &bstore->s_stats.bytes_sent, bstore->recvRate);

			// if we reused a connection from the pool, and immediately got an error, retry immediately discarding the
			// connection
			if (reqF.isReady() && reusingConn) {
				fastRetry = true;
			}

			Reference<HTTP::IncomingResponse> _r = wait(timeoutError(reqF, requestTimeout));
			r = _r;

			// Since the response was parsed successfully (which is why we are here) reuse the connection unless we
			// received the "Connection: close" header.
			if (r->data.headers["Connection"] != "close") {
				bstore->returnConnection(rconn);
			} else {
				++bstore->blobStats->expiredConnections;
			}
			rconn.conn.clear();

		} catch (Error& e) {
			TraceEvent("BlobStoreDoRequestError").errorUnsuppressed(e);
			if (e.code() == error_code_actor_cancelled)
				throw;
			// TODO: should this also do rconn.conn.clear()? (would need to extend lifetime outside of try block)
			err = e;
		}

		double end = g_network->timer();
		double connectDuration = reqStartTimer - connectStartTimer;
		double reqDuration = end - reqStartTimer;
		bstore->blobStats->requestLatency.addMeasurement(reqDuration);

		// If err is not present then r is valid.
		// If r->code is in successCodes then record the successful request and return r.
		if (!err.present() && successCodes.count(r->code) != 0) {
			bstore->s_stats.requests_successful++;
			++bstore->blobStats->requestsSuccessful;
			return r;
		}

		// Otherwise, this request is considered failed.  Update failure count.
		bstore->s_stats.requests_failed++;
		++bstore->blobStats->requestsFailed;

		// All errors in err are potentially retryable as well as certain HTTP response codes...
		bool retryable = err.present() || r->code == 500 || r->code == 502 || r->code == 503 || r->code == 429;

		// But only if our previous attempt was not the last allowable try.
		retryable = retryable && (thisTry < maxTries);

		if (!retryable || !err.present()) {
			fastRetry = false;
		}

		TraceEvent event(SevWarn,
		                 retryable ? (fastRetry ? "BlobStoreEndpointRequestFailedFastRetryable"
		                                        : "BlobStoreEndpointRequestFailedRetryable")
		                           : "BlobStoreEndpointRequestFailed");

		bool connectionFailed = false;
		// Attach err to trace event if present, otherwise extract some stuff from the response
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

		if (remoteAddress.present())
			event.detail("RemoteEndpoint", remoteAddress.get());
		else
			event.detail("RemoteHost", bstore->host);

		event.detail("Verb", verb)
		    .detail("Resource", resource)
		    .detail("ThisTry", thisTry)
		    .detail("URI", canonicalURI)
		    .detail("Proxy", bstore->proxyHost.orDefault(""));

		// If r is not valid or not code 429 then increment the try count.  429's will not count against the attempt
		// limit. Also skip incrementing the retry count for fast retries
		if (!fastRetry && (!r || r->code != 429))
			++thisTry;

		if (fastRetry) {
			++bstore->blobStats->fastRetries;
			wait(delay(0));
		} else if (retryable) {
			// We will wait delay seconds before the next retry, start with nextRetryDelay.
			double delay = nextRetryDelay;
			// conenctionFailed is treated specially as we know proxy to AWS can only serve 1 request per connection
			// so there is no point of waiting too long, instead retry more aggressively
			double limit =
			    connectionFailed ? bstore->knobs.max_delay_connection_failed : bstore->knobs.max_delay_retryable_error;
			// Double but limit the *next* nextRetryDelay.
			nextRetryDelay = std::min(nextRetryDelay * 2, limit);
			// If r is valid then obey the Retry-After response header if present.
			if (r) {
				auto iRetryAfter = r->data.headers.find("Retry-After");
				if (iRetryAfter != r->data.headers.end()) {
					event.detail("RetryAfterHeader", iRetryAfter->second);
					char* pEnd;
					double retryAfter = strtod(iRetryAfter->second.c_str(), &pEnd);
					if (*pEnd) // If there were other characters then don't trust the parsed value, use a probably safe
					           // value of 5 minutes.
						retryAfter = 300;
					// Update delay
					delay = std::max(delay, retryAfter);
				}
			}

			// Log the delay then wait.

			event.detail("RetryDelay", delay);
			wait(::delay(delay));
		} else {
			// We can't retry, so throw something.

			// This error code means the authentication header was not accepted, likely the account or key is wrong.
			if (r && r->code == 406)
				throw http_not_accepted();

			if (r && r->code == 401)
				throw http_auth_failed();

			// Recognize and throw specific errors
			if (err.present()) {
				int code = err.get().code();

				// If we get a timed_out error during the the connect() phase, we'll call that connection_failed despite
				// the fact that there was technically never a 'connection' to begin with.  It differentiates between an
				// active connection timing out vs a connection timing out, though not between an active connection
				// failing vs connection attempt failing.
				// TODO:  Add more error types?
				if (code == error_code_timed_out && !connectionEstablished) {
					TraceEvent(SevWarn, "BlobStoreEndpointConnectTimeout")
					    .suppressFor(60)
					    .detail("Timeout", requestTimeout);
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

void IBlobStoreEndpoint::returnConnection(ReusableConnection& rconn) {
	// If it expires in the future then add it to the pool in the front
	if (rconn.expirationTime > now()) {
		connectionPool->pool.push(rconn);
	} else {
		++blobStats->expiredConnections;
	}
	rconn.conn = Reference<IConnection>();
}

Future<Reference<HTTP::IncomingResponse>> IBlobStoreEndpoint::doRequest(std::string const& verb,
                                                                        std::string const& resource,
                                                                        const HTTP::Headers& headers,
                                                                        UnsentPacketQueue* pContent,
                                                                        int contentLen,
                                                                        std::set<unsigned int> successCodes) {
	return doRequest_impl(
	    Reference<IBlobStoreEndpoint>::addRef(this), verb, resource, headers, pContent, contentLen, successCodes);
}

Reference<IBlobStoreEndpoint> IBlobStoreEndpoint::fromString(const std::string& url,
                                                             const Optional<std::string>& proxy,
                                                             std::string* resourceFromURL,
                                                             std::string* error,
                                                             ParametersT* ignored_parameters) {
	if (resourceFromURL)
		resourceFromURL->clear();

	try {
		StringRef t(url);
		StringRef prefix = t.eat("://");
		if (prefix != "blobstore"_sr)
			throw format("Invalid blobstore URL prefix '%s'", prefix.toString().c_str());

		Optional<std::string> proxyHost, proxyPort;
		if (proxy.present()) {
			StringRef proxyRef(proxy.get());
			if (proxy.get().find("://") != std::string::npos) {
				StringRef proxyPrefix = proxyRef.eat("://");
				if (proxyPrefix != "http"_sr) {
					throw format("Invalid proxy URL prefix '%s'. Either don't use a prefix, or use http://",
					             proxyPrefix.toString().c_str());
				}
			}
			std::string proxyBody = proxyRef.eat().toString();
			if (!Hostname::isHostname(proxyBody) && !NetworkAddress::parseOptional(proxyBody).present()) {
				throw format("'%s' is not a valid value for proxy. Format should be either IP:port or host:port.",
				             proxyBody.c_str());
			}
			StringRef p(proxyBody);
			proxyHost = p.eat(":").toString();
			proxyPort = p.eat().toString();
		}

		Optional<StringRef> cred;
		if (url.find("@") != std::string::npos) {
			cred = t.eat("@");
		}
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

		std::string region;
		BlobKnobs knobs;
		HTTP::Headers extraHeaders;
		while (1) {
			StringRef name = t.eat("=");
			if (name.size() == 0)
				break;
			StringRef value = t.eat("&");

			// Special case for header
			if (name == "header"_sr) {
				StringRef originalValue = value;
				StringRef headerFieldName = value.eat(":");
				StringRef headerFieldValue = value;
				if (headerFieldName.size() == 0 || headerFieldValue.size() == 0) {
					throw format("'%s' is not a valid value for '%s' parameter.  Format is <FieldName>:<FieldValue> "
					             "where strings are not empty.",
					             originalValue.toString().c_str(),
					             name.toString().c_str());
				}
				std::string& fieldValue = extraHeaders[headerFieldName.toString()];
				// RFC 2616 section 4.2 says header field names can repeat but only if it is valid to concatenate their
				// values with comma separation
				if (!fieldValue.empty()) {
					fieldValue.append(",");
				}
				fieldValue.append(headerFieldValue.toString());
				continue;
			}

			// overwrite region from parameter
			if (name == "region"_sr) {
				region = value.toString();
				continue;
			}

			// See if the parameter is a knob
			// First try setting a dummy value (all knobs are currently numeric) just to see if this parameter is known
			// to IBlobStoreEndpoint. If it is, then we will set it to a good value or throw below, so the dummy set
			// has no bad side effects.
			bool known = knobs.set(name, 0);

			// If the parameter is not known to BlobStoreEndpoint then throw unless there is an ignored_parameters set
			// to add it to
			if (!known) {
				if (ignored_parameters == nullptr) {
					throw format("%s is not a valid parameter name", name.toString().c_str());
				}
				(*ignored_parameters)[name.toString()] = value.toString();
				continue;
			}

			// The parameter is known to BlobStoreEndpoint so it must be numeric and valid.
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

		return makeReference<S3BlobStoreEndpoint>(
		    host.toString(), service.toString(), region, proxyHost, proxyPort, cred, knobs, extraHeaders);

	} catch (std::string& err) {
		if (error != nullptr)
			*error = err;
		TraceEvent(SevWarnAlways, "BlobStoreEndpointBadURL")
		    .suppressFor(60)
		    .detail("Description", err)
		    .detail("Format", getURLFormat())
		    .detail("URL", url);
		throw backup_invalid_url();
	}
}
