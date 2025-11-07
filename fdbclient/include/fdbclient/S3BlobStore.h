/*
 * S3BlobStore.h
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

#pragma once

#include <map>
#include <unordered_map>
#include <functional>
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/Net2Packet.h"
#include "fdbclient/Knobs.h"
#include "flow/IRateControl.h"
#include "fdbrpc/HTTP.h"
#include "fdbclient/JSONDoc.h"
#include "flow/IConnection.h"
#include "fdbclient/IBlobStoreEndpoint.h"

#include <boost/functional/hash.hpp>


// Representation of all the things you need to connect to a blob store instance with some credentials.
// Reference counted because a very large number of them could be needed.
class S3BlobStoreEndpoint : public IBlobStoreEndpoint, ReferenceCounted<S3BlobStoreEndpoint> {
public:
	void addref() override { ReferenceCounted<S3BlobStoreEndpoint>::addref(); }
	void delref() override { ReferenceCounted<S3BlobStoreEndpoint>::delref(); }

	struct Credentials {
		std::string key;
		std::string secret;
		std::string securityToken;
	};


	S3BlobStoreEndpoint(std::string const& host,
	                    std::string const& service,
	                    std::string region,
	                    Optional<std::string> const& proxyHost,
	                    Optional<std::string> const& proxyPort,
	                    Optional<StringRef> const& credString,
	                    BlobKnobs const& knobs = BlobKnobs(),
	                    HTTP::Headers extraHeaders = HTTP::Headers());

	S3BlobStoreEndpoint(std::string const& host,
	                    std::string const& service,
	                    std::string region,
	                    Optional<std::string> const& proxyHost,
	                    Optional<std::string> const& proxyPort,
	                    Optional<Credentials> const& creds,
	                    BlobKnobs const& knobs = BlobKnobs(),
	                    HTTP::Headers extraHeaders = HTTP::Headers())
	  : host(host), service(service), region(region), proxyHost(proxyHost), proxyPort(proxyPort),
	    useProxy(proxyHost.present() && proxyPort.present()), credentials(creds),
	    lookupKey(creds.present() && creds.get().key.empty()),
	    lookupSecret(creds.present() && creds.get().secret.empty()), knobs(knobs), extraHeaders(extraHeaders),
	    requestRate(new SpeedLimit(knobs.requests_per_second, 1)),
	    requestRateList(new SpeedLimit(knobs.list_requests_per_second, 1)),
	    requestRateWrite(new SpeedLimit(knobs.write_requests_per_second, 1)),
	    requestRateRead(new SpeedLimit(knobs.read_requests_per_second, 1)),
	    requestRateDelete(new SpeedLimit(knobs.delete_requests_per_second, 1)),
	    sendRate(new SpeedLimit(knobs.max_send_bytes_per_second, 1)),
	    recvRate(new SpeedLimit(knobs.max_recv_bytes_per_second, 1)), concurrentRequests(knobs.concurrent_requests),
	    concurrentUploads(knobs.concurrent_uploads), concurrentLists(knobs.concurrent_lists) {

		if (host.empty() || (proxyHost.present() != proxyPort.present()))
			throw connection_string_invalid();

		// set connection pool instance
		if (useProxy || !knobs.global_connection_pool) {
			// don't use global connection pool if there's a proxy, as it complicates the logic
			// FIXME: handle proxies?
			connectionPool = makeReference<ConnectionPoolData>();
		} else {
			BlobStoreConnectionPoolKey key(host, service, region, knobs.isTLS());
			auto it = globalConnectionPool.find(key);
			if (it != globalConnectionPool.end()) {
				connectionPool = it->second;
			} else {
				connectionPool = makeReference<ConnectionPoolData>();
				globalConnectionPool.insert({ key, connectionPool });
			}
		}
		ASSERT(connectionPool.isValid());

		maybeStartStatsLogger();
	}

	static std::string getURLFormat(bool withResource = false) {
		const char* resource = "";
		if (withResource)
			resource = "<name>";
		return format("blobstore://<api_key>:<secret>:<security_token>@<host>[:<port>]/"
		              "%s[?<param>=<value>[&<param>=<value>]...]",
		              resource);
	}

	typedef std::map<std::string, std::string> ParametersT;

	// Parse url and return a S3BlobStoreEndpoint
	// If the url has parameters that S3BlobStoreEndpoint can't consume then an error will be thrown unless
	// ignored_parameters is given in which case the unconsumed parameters will be added to it.
	static Reference<S3BlobStoreEndpoint> fromString(const std::string& url,
	                                                 const Optional<std::string>& proxy,
	                                                 std::string* resourceFromURL,
	                                                 std::string* error,
	                                                 ParametersT* ignored_parameters);

	// Get a normalized version of this URL with the given resource and any non-default BlobKnob values as URL
	// parameters in addition to the passed params string
	std::string getResourceURL(std::string resource, std::string params) const;

	// FIXME: add periodic connection reaper to pool
	// local connection pool for this blobstore
	Reference<ConnectionPoolData> connectionPool;
	Future<ReusableConnection> connect(bool* reusingConn);
	void returnConnection(ReusableConnection& conn);

	std::string host;
	std::string service;
	std::string region;
	Optional<std::string> proxyHost;
	Optional<std::string> proxyPort;
	bool useProxy;

	Optional<Credentials> credentials;
	bool lookupKey;
	bool lookupSecret;

	// Calculates the authentication string from the secret key
	static std::string hmac_sha1(Credentials const& creds, std::string const& msg);
	Future<Void> updateSecret() override;

	void setAllAuthHeaders(const std::string& verb,
						  const std::string& resource,
						  HTTP::Headers& headers,
						  std::string date = "",
						  std::string datestamp = "") override;

	// Sets headers needed for Authorization (including Date which will be overwritten if present)
	void setAuthHeaders(std::string const& verb, std::string const& resource, HTTP::Headers& headers);

	// Set headers in the AWS V4 authorization format. $date and $datestamp are used for unit testing
	void setV4AuthHeaders(const std::string& verb,
						  const std::string& resource,
						  HTTP::Headers& headers,
						  std::string date = "",
						  std::string datestamp = "");

	std::string canonicalizeURI(const std::string& resource, std::vector<std::string>& queryParameters)  override;

	// IBlobStoreEndpoint virtual method overrides
	std::string getResourceURL(std::string resource, std::string params) const override;
	Future<Void> listObjectsStream(std::string const& bucket,
	                               PromiseStream<ListResult> results,
	                               Optional<std::string> prefix = {},
	                               Optional<char> delimiter = {},
	                               int maxDepth = 0,
	                               std::function<bool(std::string const&)> recurseFilter = nullptr) override;
	Future<ListResult> listObjects(std::string const& bucket,
	                               Optional<std::string> prefix = {},
	                               Optional<char> delimiter = {},
	                               int maxDepth = 0,
	                               std::function<bool(std::string const&)> recurseFilter = nullptr) override;
	Future<std::vector<std::string>> listBuckets() override;
	Future<bool> bucketExists(std::string const& bucket) override;
	Future<bool> objectExists(std::string const& bucket, std::string const& object) override;
	Future<int64_t> objectSize(std::string const& bucket, std::string const& object) override;
	Future<int> readObject(std::string const& bucket,
	                       std::string const& object,
	                       void* data,
	                       int length,
	                       int64_t offset) override;
	Future<Void> deleteObject(std::string const& bucket, std::string const& object) override;
	Future<Void> deleteRecursively(std::string const& bucket,
	                               std::string prefix = "",
	                               int* pNumDeleted = nullptr,
	                               int64_t* pBytesDeleted = nullptr) override;
	Future<Void> createBucket(std::string const& bucket) override;
	Future<std::string> readEntireFile(std::string const& bucket, std::string const& object) override;
	Future<Void> writeEntireFile(std::string const& bucket, std::string const& object, std::string const& content) override;
	Future<Void> writeEntireFileFromBuffer(std::string const& bucket,
	                                       std::string const& object,
	                                       UnsentPacketQueue* pContent,
	                                       int contentLen,
	                                       std::string const& contentMD5) override;
	Future<std::string> beginMultiPartUpload(std::string const& bucket, std::string const& object) override;
	Future<std::string> uploadPart(std::string const& bucket,
	                               std::string const& object,
	                               std::string const& uploadID,
	                               unsigned int partNumber,
	                               UnsentPacketQueue* pContent,
	                               int contentLen,
	                               std::string const& contentMD5) override;
	typedef std::map<int, std::string> MultiPartSetT;
	Future<Void> finishMultiPartUpload(std::string const& bucket,
	                                   std::string const& object,
	                                   std::string const& uploadID,
	                                   MultiPartSetT const& parts) override;
};
