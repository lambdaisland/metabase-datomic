ARG METABASE_VERSION=v0.32.8

FROM metabase/metabase:${METABASE_VERSION} as builder
ARG CLOJURE_CLI_VERSION=1.10.1.447

WORKDIR /app/metabase-datomic

RUN apk add --no-cache curl

ADD https://download.clojure.org/install/linux-install-${CLOJURE_CLI_VERSION}.sh ./clojure-cli-linux-install.sh
RUN chmod 744 clojure-cli-linux-install.sh
RUN ./clojure-cli-linux-install.sh
RUN rm clojure-cli-linux-install.sh

ADD https://raw.github.com/technomancy/leiningen/stable/bin/lein /usr/local/bin/lein
RUN chmod 744 /usr/local/bin/lein
RUN lein upgrade

COPY . ./

ENV CLASSPATH=/app/metabase.jar
RUN lein with-profiles +datomic-free uberjar

FROM metabase/metabase:${METABASE_VERSION} as runner

COPY --from=builder /app/metabase-datomic/target/uberjar/datomic.metabase-driver.jar /plugins
