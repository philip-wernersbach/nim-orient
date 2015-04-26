# nim-orient
OrientDB driver, uses the OrientDB 2.0 Binary Protocol with Binary
Serialization.

**Note: This is prerelease code. It is uploaded so that interested parties can
view and modify the code, and submit pull requests. The code works and you can
use it, but expect slight API breakages as we add new functionality to the
code.**

##Todo
* Generate C API for code
* Improve [variable length integer and ZigZag support](orient/src/orientpackets_unpack.nim#L87-L122)
* Optimize for performance
	* This can be done fairly easily by refactoring the code to use references
	and slices to buffers instead of copying buffers around. The current
	unpacking code essentially triple buffers everything because of its liberal
	buffer usage.
* Add support for more OrientDB features
	* The code purposely throws exceptions when it encounters unsupported
	features.

##Mission Statement
Lots of projects fail because they try to support too many features, or the
features that they support are implemented poorly. This project seeks to avoid
that fate by supporting only a subset of OrientDB's many features, and by
providing correct implementations of the features that are supported.

On the more technical side, this project targets the OrientDB 2.0 Binary
Protocol with Binary Serialization. This will ensure superior performance versus
the HTTP and CSV-based interfaces.

This project is written in the Nim programming language and it will also provide
a C API. Nim has first class interfacing with C in both directions, so the C API
will remain high quality, without needing excessive to-and-from conversions.
This also means that this project provides the OrientDB community with a high
quality driver for both Nim and C.

Overall this project provides a non-Java enterprise- and production-quality
driver for the OrientDB community.

##Usage
See [`orient/src/tests/test_all.nim`](orient/tests/test_all.nim) for an
example of using this project with OrientDB's built-in `GratefulDeadConcerts`
database.

##License
This project is licensed under the Mozilla Public License version 2.0. For full
license text, see [`LICENSE`](LICENSE).
