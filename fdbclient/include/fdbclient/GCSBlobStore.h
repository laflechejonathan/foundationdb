/*
 * GCSBlobStore.h
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

#include "fdbclient/IBlobStoreEndpoint.h"

#include <map>
#include <queue>
#include "flow/flow.h"
#include "fdbclient/Knobs.h"
#include "flow/IRateControl.h"
#include "fdbrpc/HTTP.h"
#include "flow/IConnection.h"
#include "fdbrpc/Stats.h"

class GCSBlobStoreEndpoint : public IBlobStoreEndpoint, ReferenceCounted<GCSBlobStoreEndpoint> {
public:
	void addref() override { ReferenceCounted<GCSBlobStoreEndpoint>::addref(); }
	void delref() override { ReferenceCounted<GCSBlobStoreEndpoint>::delref(); }

	struct Credentials {
		std::string token;
	};

	struct ReusableConnection {
		Reference<IConnection> conn;
		double expirationTime;
	};

	struct ConnectionPoolData : NonCopyable, ReferenceCounted<ConnectionPoolData> {
		std::queue<ReusableConnection> pool;
	};

	struct Stats {
		Stats() : requests_successful(0), requests_failed(0), bytes_sent(0) {}
		Stats operator-(const Stats& rhs);
		void clear() { memset(this, 0, sizeof(*this)); }
		json_spirit::mObject getJSON();

		int64_t requests_successful;
		int64_t requests_failed;
		int64_t bytes_sent;
	};

	static Stats s_stats;

	struct BlobStats {
		UID id;
		CounterCollection cc;
		Counter requestsSuccessful;
		Counter requestsFailed;
		Counter newConnections;
		Counter expiredConnections;
		Counter reusedConnections;
		Counter fastRetries;
		LatencySample requestLatency;

		BlobStats();
	};

	static std::unique_ptr<BlobStats> blobStats;
	static Future<Void> statsLogger;

	GCSBlobStoreEndpoint(std::string const& host,
	                    std::string const& service,
	                    Optional<std::string> const& proxyHost,
	                    Optional<std::string> const& proxyPort,
	                    Optional<Credentials> const& creds,
	                    BlobKnobs const& knobs = BlobKnobs(),
	                    HTTP::Headers extraHeaders = HTTP::Headers())
	  : IBlobStoreEndpoint(host, service, "auto", proxyHost, proxyPort, knobs, extraHeaders), credentials(creds),
	    lookupToken(creds.present() && creds.get().token.empty()) {
	}

	static std::string getURLFormat(bool withResource = false) {
		const char* resource = "";
		if (withResource)
			resource = "<bucket>/<name>";
		return std::string("blobstore://") + "<host>[:<port>]/" + resource + "?gcs=1[&<param>=<value>...]";
	}

	// Check if a URL is a GCS URL (contains gcs=1 parameter)
	static bool isGCSURL(const std::string& url);

	// Parse url and return a GCSBlobStoreEndpoint
	// If the url has parameters that GCSBlobStoreEndpoint can't consume then an error will be thrown unless
	// ignored_parameters is given in which case the unconsumed parameters will be added to it.
	static Reference<GCSBlobStoreEndpoint> fromString(const std::string& url,
	                                                 const Optional<std::string>& proxy,
	                                                 std::string* resourceFromURL,
	                                                 std::string* error,
	                                                 ParametersT* ignored_parameters);

	// Get a normalized version of this URL with the given resource and any non-default BlobKnob values as URL
	// parameters in addition to the passed params string
	std::string getResourceURL(std::string resource, std::string params) const override;

	std::string canonicalizeURI(const std::string& resource, std::vector<std::string>& queryParameters)  override;

	static Credentials loadCredentialsFromFile(std::string const& filename);

	Optional<Credentials> credentials;
	bool lookupToken;

	Future<Void> updateSecret() override;
	void setAllAuthHeaders(const std::string& verb,
						  const std::string& resource,
						  HTTP::Headers& headers,
						  std::string date = "",
						  std::string datestamp = "") override;

	// IBlobStoreEndpoint virtual method overrides
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
	Future<std::string> readEntireFile(std::string const& bucket, std::string const& object) override;
	Future<Void> writeEntireFile(std::string const& bucket,
	                              std::string const& object,
	                              std::string const& content) override;
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
	Future<Void> finishMultiPartUpload(std::string const& bucket,
	                                    std::string const& object,
	                                    std::string const& uploadID,
	                                    MultiPartSetT const& parts) override;
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
	Future<Void> createBucket(std::string const& bucket) override;
};
