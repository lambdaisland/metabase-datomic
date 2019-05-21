# metabase-datomic

<!-- badges -->
[![CircleCI](https://circleci.com/gh/plexus/metabase-datomic.svg?style=svg)](https://circleci.com/gh/plexus/metabase-datomic) [![cljdoc badge](https://cljdoc.org/badge/plexus/metabase-datomic)](https://cljdoc.org/d/plexus/metabase-datomic) [![Clojars Project](https://img.shields.io/clojars/v/plexus/metabase-datomic.svg)](https://clojars.org/plexus/metabase-datomic)
<!-- /badges -->

A Metabase driver for Datomic.

Work in progress. Sponsored by Eleven.

## Design decisions

See the [Architecture Decision Log](doc/architecture_decision_log.org)

## Developing

To get a REPL based workflow, do a git clone of both `metabase` and
`metabase-datomic`, such that they are in sibling directories

``` shell
$ git clone git@github.com:metabase/metabase.git
$ git clone git@github.com:plexus/metabase-datomic.git

$ tree -L 1
.
│
├── metabase
└── metabase-datomic
```

Before you can use Metabase you need to build the frontend. This step you only
need to do once.

``` shell
cd metabase
yarn build
```

Now `cd` into the `metabase-datomic` directory, and run `bin/start_metabase` to
lauch the process including nREPL running on port 4444.

``` shell
cd metabase-datomic
bin/start_metabase
```

Now you can connect from Emacs/CIDER to port 4444, or use the
`bin/cider_connect` script to automatically connect, and to associate the REPL
session with both projects, so you can easily evaluate code in either, and
navigate back and forth.

Once you have a REPL you can start the web app. This also opens a browser at
`localhost:3000`

``` clojure
user=> (go)
```

The first time it will ask you to create a user and do some other initial setup.
To skip this step, invoke `setup!`. This will create a user with username
`arne@example.com` and password `dev`. It will also create a Datomic database
with URL `datomic:free://localhost:4334/mbrainz`. You are encouraged to run a
datomic-free transactor, and
[import the MusicBrainz](https://github.com/Datomic/mbrainz-sample)
database for testing.

``` clojure
user=> (setup!)
```

## Installing

The general process is to build an uberjar, and copy the result into your
Metabase `plugins/` directory. You can build a jar based on datomic-free, or
datomic-pro (assuming you have a license).

``` shell
cd metabase-datomic
lein with-profiles +datomic-free uberjar
# lein with-profiles +datomic-pro uberjar
cp target/uberjar/datomic.metabase-driver.jar ../metabase/plugins
```

Now you can start Metabase, and start adding Datomic databases

``` shell
cd ../metabase
lein run -m metabase.core
```

## Status

<!-- feature-table -->
<table><tr><th align='left'>Feature</th><th align='center'>Supported?</th></tr><tr><th align='left'>Basics</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:source-table integer-literal}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:fields [&amp; field]}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:field-id field-id]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:datetime-field local-field | fk unit]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:breakout [&amp; concrete-field]}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:field-id field-id]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:aggregation 0]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:datetime-field local-field | fk unit]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:filter filter-clause}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:and &amp; filter-clause]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:or &amp; filter-clause]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:not filter-clause]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:= concrete-field value &amp; value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:!= concrete-field value &amp; value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&lt; concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&gt; concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&lt;= concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&gt;= concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:is-null concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:not-null concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:between concrete-field min orderable-value max orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:inside lat concrete-field lon concrete-field lat-max numeric-literal lon-min numeric-literal lat-min numeric-literal lon-max numeric-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:starts-with concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:contains concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:does-not-contain concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:ends-with concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:time-interval field concrete-field n :current|:last|:next|integer-literal unit relative-datetime-unit]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:limit integer-literal}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:order-by [&amp; order-by-clause]}</td><td align='center'>Yes</td></tr><tr><th align='left'>:basic-aggregations</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:aggregation aggregation-clause}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:count]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:count concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:cum-count concrete-field]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:cum-sum concrete-field]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:distinct concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:sum concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:min concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:max concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:share filter-clause]</td><td align='center'></td></tr><tr><th align='left'>:standard-deviation-aggregations</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:aggregation aggregation-clause}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:stddev concrete-field]</td><td align='center'>Yes</td></tr><tr><th align='left'>:foreign-keys</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:fields [&amp; field]}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:fk-&gt; fk-field-id dest-field-id]</td><td align='center'>Yes</td></tr><tr><th align='left'>:nested-fields</th><th align='center'></th></tr><tr><th align='left'>:set-timezone</th><th align='center'></th></tr><tr><th align='left'>:expressions</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:fields [&amp; field]}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:expression]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:breakout [&amp; concrete-field]}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:expression]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:expressions {expression-name expression}}</td><td align='center'></td></tr><tr><th align='left'>:native-parameters</th><th align='center'></th></tr><tr><th align='left'>:expression-aggregations</th><th align='center'></th></tr><tr><th align='left'>:nested-queries</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:source-query query}</td><td align='center'>Yes</td></tr><tr><th align='left'>:binning</th><th align='center'></th></tr><tr><th align='left'>:case-sensitivity-string-filter-options</th><th align='center'></th></tr></table>
<!-- /feature-table -->

## License

Copyright &copy; 2019 Arne Brasseur

Licensed under the term of the Mozilla Public License 2.0, see LICENSE.
