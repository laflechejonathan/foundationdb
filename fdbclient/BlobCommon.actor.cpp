//
// Created by Jonathan Lafleche on 5/11/25.
//
#include "fdbclient/IBlobStoreEndpoint.h"
#include "flow/IAsyncFile.h"
#include "flow/actorcompiler.h" // has to be last include

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

