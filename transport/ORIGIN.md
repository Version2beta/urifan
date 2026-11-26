# Bundled transport source

This directory contains the urihandler code written for this project's owner,
now included in urifan under the repository's MIT license. It is not a download
of the older library with the same historical module path.

Original source revision: `941401e062cd93280e8de869651aa22078df3f27`.
`UPSTREAM.sha256` records the original production Go file hashes. Production
source was copied unchanged; external test import paths were changed to
`github.com/Version2beta/urifan/transport`. This directory retains the original
Go package name `urihandler`; callers commonly import it with alias `uri`.

Keeping the source in this module makes published library imports and CLI builds
self-contained. No sibling checkout, local module replacement or inaccessible
external module version is required. The original development checkout is not
modified or separately published by this change.
