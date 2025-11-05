/*
 * IBlobStoreEndpoint.h
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

#include <functional>
#include <map>
#include <vector>
#include "flow/flow.h"
#include "fdbclient/JSONDoc.h"

class UnsentPacketQueue;

struct BlobKnobs {
	BlobKnobs();
	int secure_connection, connect_tries, connect_timeout, max_connection_life, request_tries, request_timeout_min,
		requests_per_second, list_requests_per_second, write_requests_per_second, read_requests_per_second,
		delete_requests_per_second, multipart_max_part_size, multipart_min_part_size, concurrent_requests,
		concurrent_uploads, concurrent_lists, concurrent_reads_per_file, concurrent_writes_per_file,
		enable_read_cache, read_block_size, read_ahead_blocks, read_cache_blocks_per_file,
		max_send_bytes_per_second, max_recv_bytes_per_second, sdk_auth, global_connection_pool,
		max_delay_retryable_error, max_delay_connection_failed;

	bool set(StringRef name, int value);
	std::string getURLParameters() const;
	static std::vector<std::string> getKnobDescriptions() {
		return {
			"secure_connection (or sc)             Set 1 for secure connection and 0 for insecure connection.",
			"connect_tries (or ct)                 Number of times to try to connect for each request.",
			"connect_timeout (or cto)              Number of seconds to wait for a connect request to succeed.",
			"max_connection_life (or mcl)          Maximum number of seconds to use a single TCP connection.",
			"request_tries (or rt)                 Number of times to try each request until a parsable HTTP "
			"response other than 429 is received.",
			"request_timeout_min (or rtom)         Number of seconds to wait for a request to succeed after a "
			"connection is established.",
			"requests_per_second (or rps)          Max number of requests to start per second.",
			"list_requests_per_second (or lrps)    Max number of list requests to start per second.",
			"write_requests_per_second (or wrps)   Max number of write requests to start per second.",
			"read_requests_per_second (or rrps)    Max number of read requests to start per second.",
			"delete_requests_per_second (or drps)  Max number of delete requests to start per second.",
			"multipart_max_part_size (or maxps)    Max part size for multipart uploads.",
			"multipart_min_part_size (or minps)    Min part size for multipart uploads.",
			"concurrent_requests (or cr)           Max number of total requests in progress at once, regardless of "
			"operation-specific concurrency limits.",
			"concurrent_uploads (or cu)            Max concurrent uploads (part or whole) that can be in progress "
			"at once.",
			"concurrent_lists (or cl)              Max concurrent list operations that can be in progress at once.",
			"concurrent_reads_per_file (or crps)   Max concurrent reads in progress for any one file.",
			"concurrent_writes_per_file (or cwps)  Max concurrent uploads in progress for any one file.",
			"enable_read_cache (or erc)            Whether read block caching is enabled.",
			"read_block_size (or rbs)              Block size in bytes to be used for reads.",
			"read_ahead_blocks (or rab)            Number of blocks to read ahead of requested offset.",
			"read_cache_blocks_per_file (or rcb)   Size of the read cache for a file in blocks.",
			"max_send_bytes_per_second (or sbps)   Max send bytes per second for all requests combined.",
			"max_recv_bytes_per_second (or rbps)   Max receive bytes per second for all requests combined (NOT YET "
			"USED).",
			"max_delay_retryable_error (or dre)    Max seconds to delay before retry when see a retryable error.",
			"max_delay_connection_failed (or dcf)  Max seconds to delay before retry when see a connection "
			"failure.",
			"sdk_auth (or sa)                      Use AWS SDK to resolve credentials. Only valid if "
			"BUILD_AWS_BACKUP is enabled.",
			"global_connection_pool (or gcp)       Enable shared connection pool between all blobstore instances."
		};
	}

	bool isTLS() const { return secure_connection == 1; }
};

class IBlobStoreEndpoint {
public:
	virtual ~IBlobStoreEndpoint() {}
	virtual void addref() = 0;
	virtual void delref() = 0;

	struct ObjectInfo {
		std::string name;
		int64_t size;
	};

	struct ListResult {
		std::vector<std::string> commonPrefixes;
		std::vector<ObjectInfo> objects;
	};

	typedef std::map<std::string, std::string> ParametersT;
	typedef std::map<int, std::string> MultiPartSetT;

	virtual Future<bool> bucketExists(std::string const& bucket) = 0;
	virtual Future<bool> objectExists(std::string const& bucket, std::string const& object) = 0;
	virtual Future<int64_t> objectSize(std::string const& bucket, std::string const& object) = 0;
	virtual Future<int> readObject(std::string const& bucket,
	                                std::string const& object,
	                                void* data,
	                                int length,
	                                int64_t offset) = 0;
	virtual Future<Void> deleteObject(std::string const& bucket, std::string const& object) = 0;
	virtual Future<Void> deleteRecursively(std::string const& bucket,
	                                        std::string prefix = "",
	                                        int* pNumDeleted = nullptr,
	                                        int64_t* pBytesDeleted = nullptr) = 0;
	virtual Future<std::string> readEntireFile(std::string const& bucket, std::string const& object) = 0;
	virtual Future<Void> writeEntireFile(std::string const& bucket,
	                                      std::string const& object,
	                                      std::string const& content) = 0;
	virtual Future<Void> writeEntireFileFromBuffer(std::string const& bucket,
	                                                std::string const& object,
	                                                UnsentPacketQueue* pContent,
	                                                int contentLen,
	                                                std::string const& contentMD5) = 0;

	virtual Future<std::string> beginMultiPartUpload(std::string const& bucket, std::string const& object) = 0;
	virtual Future<std::string> uploadPart(std::string const& bucket,
	                                        std::string const& object,
	                                        std::string const& uploadID,
	                                        unsigned int partNumber,
	                                        UnsentPacketQueue* pContent,
	                                        int contentLen,
	                                        std::string const& contentMD5) = 0;
	virtual Future<Void> finishMultiPartUpload(std::string const& bucket,
	                                            std::string const& object,
	                                            std::string const& uploadID,
	                                            MultiPartSetT const& parts) = 0;

	virtual Future<Void> listObjectsStream(std::string const& bucket,
	                                        PromiseStream<ListResult> results,
	                                        Optional<std::string> prefix = {},
	                                        Optional<char> delimiter = {},
	                                        int maxDepth = 0,
	                                        std::function<bool(std::string const&)> recurseFilter = nullptr) = 0;

	virtual Future<ListResult> listObjects(std::string const& bucket,
	                                        Optional<std::string> prefix = {},
	                                        Optional<char> delimiter = {},
	                                        int maxDepth = 0,
	                                        std::function<bool(std::string const&)> recurseFilter = nullptr) = 0;

	virtual Future<std::vector<std::string>> listBuckets() = 0;
	virtual Future<Void> createBucket(std::string const& bucket) = 0;

	// Get a normalized version of this URL with the given resource and any non-default BlobKnob values as URL
	// parameters in addition to the passed params string
	virtual std::string getResourceURL(std::string resource, std::string params) const = 0;
	virtual Future<Void> updateSecret() = 0;

	BlobKnobs knobs;
};

Future<Optional<json_spirit::mObject>> tryReadJSONFile(std::string path);
