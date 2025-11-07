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
#include "flow/flow.h"
#include "fdbclient/Knobs.h"
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

	GCSBlobStoreEndpoint(std::string const& host,
	                    std::string const& service,
	                    Optional<std::string> const& proxyHost,
	                    Optional<std::string> const& proxyPort,
	                    Optional<StringRef> const& creds,
	                    BlobKnobs const& knobs = BlobKnobs(),
	                    HTTP::Headers extraHeaders = HTTP::Headers());

	Optional<Credentials> credentials;
	bool lookupToken;

	std::string getResourceURL(std::string resource, std::string params) const override;
	std::string normalizeURIForRemoteRequest(const std::string& resource) override;
	Future<Void> updateSecret() override;
	bool lookupSecretOnEachRequest() override;
	void setAllRequestHeaders(const std::string& verb,
	                          const std::string& resource,
	                          HTTP::Headers& headers,
	                          std::string date = "",
	                          std::string datestamp = "") override;
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
