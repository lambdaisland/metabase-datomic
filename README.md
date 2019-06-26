# metabase-datomic

<!-- badges -->
<!-- [![CircleCI](https://circleci.com/gh/plexus/metabase-datomic.svg?style=svg)](https://circleci.com/gh/plexus/metabase-datomic) [![cljdoc badge](https://cljdoc.org/badge/plexus/metabase-datomic)](https://cljdoc.org/d/plexus/metabase-datomic) [![Clojars Project](https://img.shields.io/clojars/v/plexus/metabase-datomic.svg)](https://clojars.org/plexus/metabase-datomic) -->
<!-- /badges -->

A Metabase driver for Datomic.

Commercial support is provided by [Gaiwan](http://gaiwan.co).

## Try it!

```
docker run -p 3000:3000 lambdaisland/metabase-datomic
```

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

And install metabase locally

``` shell
lein install
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

The general process is to build an uberjar, and copy the result into
your Metabase `plugins/` directory. You can build a jar based on
datomic-free, or datomic-pro (assuming you have a license). Metabase
must be available as a local JAR.

``` shell
cd metabase
lein install
mkdir plugins
cd ../metabase-datomic
lein with-profiles +datomic-free uberjar
# lein with-profiles +datomic-pro uberjar
cp target/uberjar/datomic.metabase-driver.jar ../metabase/plugins
```

Now you can start Metabase, and start adding Datomic databases

``` shell
cd ../metabase
lein run -m metabase.core
```

## Configuration EDN

When you configure a Datomic Database in Metabase you will notice a config field
called "Configuration EDN". Here you can paste a snippet of EDN which will
influence some of the Driver's behavior.

The EDN needs to represent a Clojure map. These keys are currently understood

- `:inclusion-clauses`
- `:tx-filter`
- `:relationships`

Other keys are ignored.

### `:inclusion-clauses`

Datomic does not have tables, but nevertheless the driver will map your data to
Metabase tables based on the attribute names in your schema. To limit results to
the right entities it needs to do a check to see if a certain entity logically
belongs to such a table.

By default these look like this

``` clojure
[(or [?eid :user/name]
     [?eid :user/password]
     [?eid :user/roles])]
```

In other words we look for entities that have any attribute starting with the
given prefix. This can be both suboptimal (there might be a single attribute
with an index that is faster to check), and it may be wrong, depending on your
setup.

So we allow configuring this clause per table. The configured value should be a
vector of datomic clauses. You have the full power of datalog available. Use the
special symbol `?eid` for the entity that is being filtered.

``` clojure
{:inclusion-clauses {"user" [[?eid :user/handle]]}}
```

### `:tx-filter`

The `datomic.api/filter` function allows you to get a filtered view of the
database. A common use case is to select datoms based on metadata added to
transaction entities.

You can set `:tx-filter` to any form that evaluates to a Clojure function. Make
sure any namespaces like `datomic.api` are fully qualified.

``` clojure
{:tx-filter
 (fn [db ^datomic.Datom datom]
   (let [tx-user (get-in (datomic.api/entity db (.tx datom)) [:tx/user :db/id])]
     (or (nil? tx-tenant) (= 17592186046521 tx-user))))}
```

### `:rules`

This allows you to configure Datomic rules. These then become available in the
native query editor, as well in `:inclusion-clauses` and `:relationships`.

``` clojure
{:rules
 [[(sub-accounts ?p ?c)
   [?p :account/children ?c]]
  [(sub-accounts ?p ?d)
   [?p :account/children ?c]
   (sub-accounts ?c ?d)]]}
```

### `:relationships`

This features allows you to add "synthetic foreign keys" to tables. These are
fields that Metabase will consider to be foreign keys, but in reality they are
backed by an arbitrary lookup path in Datomic. This can include reverse
reference (`:foo/_bar`) and rules.

To set up an extra relationship you start from the table where you want to add
the relationship, then give it a name, give the path of attributes and rules
needed to get to the other entity, and specifiy which table the resulting entity
belongs to.

``` clojure
{:relationships
 {;; foreign keys added to the account table
  :account
  {:journal-entry-lines
   {:path [:journal-entry-line/_account]
    :target :journal-entry-line}

   :subaccounts
   {:path [sub-accounts]
    :target :account}

   :parent-accounts
   {:path [_sub-accounts] ;; apply a rule in reverse
    :target :account}}

  ;; foreign keys added to the journal-entry-line table
  :journal-entry-line
  {:fiscal-year
   {:path [:journal-entry/_journal-entry-lines
           :ledger/_journal-entries
           :fiscal-year/_ledgers]
    :target :fiscal-year}}}}
```

## Status

<!-- feature-table -->
<table><tr><th align='left'>Feature</th><th align='center'>Supported?</th></tr><tr><th align='left'>Basics</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:source-table integer-literal}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:fields [&amp; field]}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:field-id field-id]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:datetime-field local-field | fk unit]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:breakout [&amp; concrete-field]}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:field-id field-id]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:aggregation 0]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:datetime-field local-field | fk unit]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:filter filter-clause}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:and &amp; filter-clause]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:or &amp; filter-clause]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:not filter-clause]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:= concrete-field value &amp; value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:!= concrete-field value &amp; value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&lt; concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&gt; concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&lt;= concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:&gt;= concrete-field orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:is-null concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:not-null concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:between concrete-field min orderable-value max orderable-value]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:inside lat concrete-field lon concrete-field lat-max numeric-literal lon-min numeric-literal lat-min numeric-literal lon-max numeric-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:starts-with concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:contains concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:does-not-contain concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:ends-with concrete-field string-literal]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:time-interval field concrete-field n :current|:last|:next|integer-literal unit relative-datetime-unit]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:limit integer-literal}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:order-by [&amp; order-by-clause]}</td><td align='center'>Yes</td></tr><tr><th align='left'>:basic-aggregations</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:aggregation aggregation-clause}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:count]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:count concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:cum-count concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:cum-sum concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:distinct concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:sum concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:min concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:max concrete-field]</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:share filter-clause]</td><td align='center'></td></tr><tr><th align='left'>:standard-deviation-aggregations</th><th align='center'>Yes</th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:aggregation aggregation-clause}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:stddev concrete-field]</td><td align='center'>Yes</td></tr><tr><th align='left'>:foreign-keys</th><th align='center'>Yes</th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:fields [&amp; field]}</td><td align='center'>Yes</td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:fk-&gt; fk-field-id dest-field-id]</td><td align='center'>Yes</td></tr><tr><th align='left'>:nested-fields</th><th align='center'></th></tr><tr><th align='left'>:set-timezone</th><th align='center'></th></tr><tr><th align='left'>:expressions</th><th align='center'></th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:fields [&amp; field]}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:expression]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:breakout [&amp; concrete-field]}</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[:expression]</td><td align='center'></td></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:expressions {expression-name expression}}</td><td align='center'></td></tr><tr><th align='left'>:native-parameters</th><th align='center'></th></tr><tr><th align='left'>:expression-aggregations</th><th align='center'></th></tr><tr><th align='left'>:nested-queries</th><th align='center'>Yes</th></tr><tr><td align='left'>&nbsp;&nbsp;&nbsp;&nbsp;{:source-query query}</td><td align='center'>Yes</td></tr><tr><th align='left'>:binning</th><th align='center'></th></tr><tr><th align='left'>:case-sensitivity-string-filter-options</th><th align='center'>Yes</th></tr></table>
<!-- /feature-table -->

## License

Copyright &copy; 2019 Arne Brasseur

Licensed under the term of the Mozilla Public License 2.0, see LICENSE.
