s3s3mirror
==========


A utility for mirroring content from one S3 bucket to another.

Designed to be lightning-fast and highly concurrent, with modest CPU and memory requirements.

An object will be copied if and only if at least one of the following holds true:

* The object does not exist in the destination bucket.
* The size or ETag of the object in the destination bucket are different from the size/ETag in the source bucket.

When copying, the source metadata and ACL lists are also copied to the destination object.

### Motivation

I started with "s3cmd sync" but found that with buckets containing many thousands of objects, it was incredibly slow
to start and consumed *massive* amounts of memory. So I designed s3s3mirror to start copying immediately with an intelligently
chosen "chunk size" and to operate in a highly-threaded, streaming fashion, so memory requirements are much lower.

Running with 100 threads, I found the gating factor to be *how fast I could list items from the source bucket* (!?!)
Which makes me wonder if there is any way to do this faster. I'm sure there must be, but this is pretty damn fast.

### AWS Credentials

* s3s3mirror will first look for credentials in your system environment. If variables named AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY are defined, then these will be used.
* Next, it checks for a ~/.s3cfg file (which you might have for using s3cmd). If present, the access key and secret key are read from there.
* If neither of the above is found, it will error out and refuse to run.

### System Requirements

* Java 6

### Building

    mvn package

Note that s3s3mirror now has a prebuilt jar checked in to github, so you'll only need to do this if you've been playing with the source code.
The above command requires that Maven 3 is installed.

### Usage

    s3s3mirror.sh [options] <source-bucket>[/src-prefix/path/...] <destination-bucket>[/dest-prefix/path/...]

### Options

    -c (--ctime) N           : Only copy objects whose Last-Modified date is younger than this many days
                               For other time units, use these suffixes: y (years), M (months), d (days), w (weeks),
                                                                         h (hours), m (minutes), s (seconds)
    -m (--max-connections) N  : Maximum number of connections to S3 (default 100)
    -n (--dry-run)            : Do not actually do anything, but show what would be done (default false)
    -r (--max-retries) N      : Maximum number of retries for S3 requests (default 5)
    -p (--prefix) VAL         : Only copy objects whose keys start with this prefix
    -d (--dest-prefix) VAL    : Destination prefix (replacing the one specified in --prefix, if any)
    -e (--endpoint) VAL       : AWS endpoint to use (or set AWS_ENDPOINT in your environment)
    -X (--delete-removed)     : Delete objects from the destination bucket if they do not exist in the source bucket
    -t (--max-threads) N      : Maximum number of threads (default 100)
    -v (--verbose)            : Verbose output (default false)
    -z (--proxy) VAL          : host:port of proxy server to use.
                                Defaults to proxy_host and proxy_port defined in ~/.s3cfg,
                                or no proxy if these values are not found in ~/.s3cfg
    -u (--upload-part-size) N : The upload size (in bytes) of each part uploaded as part of a multipart request
                                for files that are greater than the max allowed file size of 5368709120 bytes (5 GB)
                                Defaults to 4294967296 bytes (4 GB)
    -C (--cross-account-copy) : Copy across AWS accounts. Only Resource-based policies are supported (as
                                specified by AWS documentation) for cross account copying
                                Default is false (copying within same account, preserving ACLs across copies)
                                If this option is active, the owner of the destination bucket will receive full control


### Examples

Copy everything from a bucket named "source" to another bucket named "dest"

    s3s3mirror.sh source dest

Copy everything from "source" to "dest", but only copy objects created or modified within the past week

    s3s3mirror.sh -c 7 source dest
    s3s3mirror.sh -c 7d source dest
    s3s3mirror.sh -c 1w source dest
    s3s3mirror.sh --ctime 1w source dest

Copy everything from "source/foo" to "dest/bar"

    s3s3mirror.sh source/foo dest/bar
    s3s3mirror.sh -p foo -d bar source dest

Copy everything from "source/foo" to "dest/bar" and delete anything in "dest/bar" that does not exist in "source/foo"

    s3s3mirror.sh -X source/foo dest/bar
    s3s3mirror.sh --delete-removed source/foo dest/bar
    s3s3mirror.sh -p foo -d bar -X source dest
    s3s3mirror.sh -p foo -d bar --delete-removed source dest

Copy within a single bucket -- copy everything from "source/foo" to "source/bar"

    s3s3mirror.sh source/foo source/bar
    s3s3mirror.sh -p foo -d bar source source

BAD IDEA: If copying within a single bucket, do *not* put the destination below the source

    s3s3mirror.sh source/foo source/foo/subfolder
    s3s3mirror.sh -p foo -d foo/subfolder source source
*This might cause recursion and raise your AWS bill unnecessarily*

