s3s3mirror
==========

A utility for mirroring content from one S3 bucket to another.

Designed to be lightning-fast and highly concurrent, with modest CPU and memory requirements.

### AWS Credentials

* s3s3mirror will first look for credentials in your system environment. If variables named AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY are defined, then these will be used.
* Next, it checks for a ~/.s3cfg file (which you might have for using s3cmd). If present, the access key and secret key are read from there.
* If neither of the above is found, it will error out and refuse to run.

### System Requirements

* Java 7
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

