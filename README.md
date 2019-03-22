# metabase-datomic

<!-- badges -->
[![CircleCI](https://circleci.com/gh/plexus/metabase-datomic.svg?style=svg)](https://circleci.com/gh/plexus/metabase-datomic) [![cljdoc badge](https://cljdoc.org/badge/plexus/metabase-datomic)](https://cljdoc.org/d/plexus/metabase-datomic) [![Clojars Project](https://img.shields.io/clojars/v/plexus/metabase-datomic.svg)](https://clojars.org/plexus/metabase-datomic)
<!-- /badges -->

A Metabase driver for Datomic.

Work in progress. Sponsored by Eleven.

## Design decisions

There is a mismatch between Metabase's table+column view of the world, and
Datomic's mix-and-match entity+properties approach. This driver uses certain
heuristics to map the latter onto the former.

Any attribute prefix (namespace) is considered a table name. Any entity that has
an attribute with that prefix is considered to be a "row" in that table. Any
attribute in any such matching entity is considered a column of said table.

## Developing

To get a REPL based workflow, do a git clone of both `metabase` and `metabase-datomic`, such that they are in sibling directories

```
.
│
├── metabase
└── metabase-datomic
```

Before you can use Metabase you need to build the frontend. This step you only
need to do once.

```
cd metabase
yarn build
```

Now `cd` into the `metabase-datomic` directory, and run `bin/start_metabase` to
lauch the process including nREPL running on port 4444.

```
cd metabase-datomic
bin/start_metabase
```

Now you can connect from Emacs/CIDER to port 4444, or use the
`bin/cider_connect` script to automatically connect, and to associate the REPL
session with both projects, so you can easily evaluate code in either, and
navigate back and forth.

Once you have a REPL you can start the web app.

``` clojure
user=> (start-metabase!)
```

And open it in the browser at `localhost:3000`

``` clojure
user=> (open-metabase)
```

The first time it will ask you to create a user and do some other initial setup.
To skip this step, invoke `initial-setup!`. This will create a user with
username `arne@example.com` and password `dev`. It will also create a Datomic
database with URL `datomic:free://localhost:4334/mbrainz`. You are encouraged to
run a datomic-free transactor, and import the MusicBrainz database for testing.

```
user=> (initial-setup!)
```

## Installing

Run `lein jar` and copy the result into your Metabase `plugins/` directory.

```
cd metabase-datomic
lein jar
cp target/*.jar ../metabase/plugins

cd ../metabase-datomic
lein run -m metabase.core
```

## License

Copyright &copy; 2019 Arne Brasseur

Licensed under the term of the Mozilla Public License 2.0, see LICENSE.
