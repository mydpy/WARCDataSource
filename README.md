# WARCDataSource

Work in progress.

Spark DataSourceV2 for reading [WARC](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/#warc-record).

## Unit Tests

The unit test is definitely not self contained since it relies on having [one of the April 2019 WARC files](https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-18/segments/1555578517558.8/warc/CC-MAIN-20190418101243-20190418122315-00032.warc.gz)
present in `$HOME/Downloads`. The gradle `runSpark` task also assumes this. This is due to line ending peculiarities 
between Unix and Windows and making certain that trying to grab a few records wasn't resulting in problems I experienced 
while parsing records.
