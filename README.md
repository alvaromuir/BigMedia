# BigMedia

BigMedia is an Apache-Spark powered big data platform for analyzing online media metrics as reported from Google's DoubleClick Marketing Manager. BigMedia exists because:

  - DBM reporting is not that fast
  - Does not allow real or even near time analytics
  - Large Excel files are not data science

### Version
0.0.1

### Tech

BigMedia relies on a number of open source projects:

* [Apache Spark] - Cluster computing framework
* [Apache Hadoop] - Distributed storage and data processing
* [Scala] - Functional programming via the JVM

### Installation

Grab the repo, clean, compile then package the SBT project:

```sh
$ git clone http://www.github.com/alvaromuir/BigMedia
$ cd BigMedia
$ sbt clean
$ sbt compile
$ sbt package
```


Questions: Hit me. @alvaromuir


[Apache Spark]:https://spark.apache.org/
[Apache Hadoop]:http://hadoop.apache.org/
[Scala]:http://www.scala-lang.org/