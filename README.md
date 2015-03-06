SQL Parser written in Scala
===========================

This is an attempt to build a functional SQL parser which resides outside
the context of a specific database system.

So far, only `SELECT` statements are implemented, but more is to come.

Why do I fork this repo from [stephentu](https://github.com/stephentu/scala-sql-parser) ?
--------

I'm working at a project using spark sql and parquet. After everything is built, I face the fact that it's hard to calculate the data size processed by spark sql.
Certainly, there are some elegant solutions to solve that problem. Parsing the sql and parquet meta data, will lead to the final answer.

Building
--------

This project is built with `sbt`:

    sbt compile
    sbt test

You can also install it in your local sbt repo directory: ~/.sbt

    sbt install