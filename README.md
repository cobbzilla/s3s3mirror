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
* Maven 3

### Building

    mvn package

### Usage

    s3s3mirror.sh [options] <source-bucket> <destination-bucket>

### Options

    -m (--max-connections) N : Maximum number of connections to S3 (default 100)
    -n (--dry-run)           : Do not actually do anything, but show what would be done (default false)
    -t (--max-threads) N     : Maximum number of threads (default is same as --max-connections)
    -v (--verbose)           : Verbose output (default false)

