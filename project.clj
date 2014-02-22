(defproject passim "0.1.0-SNAPSHOT"
  :description "Detect and align similar passages"
  :jvm-opts ["-server" "-Dfile.encoding=UTF-8" "-Xmx1900m"]
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["edu.umass.ciir.releases"
                  {:url "http://scm-ciir.cs.umass.edu:8080/nexus/content/repositories/releases"
                   :snapshots false}]
                 ["edu.umass.ciir.snapshots"
                  "http://scm-ciir.cs.umass.edu:8080/nexus/content/repositories/snapshots"]]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.csv "0.1.2"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/math.combinatorics "0.0.4"]
		 [ciir/utils "1.0.0-SNAPSHOT"]
                 [org.lemurproject.galago/core "3.5.2"]]
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :plugins [[lein-bin "0.3.4"]]
  :bin {:name "passim"
        :bin-path "~/bin"}
  :aot [passim]
  :main passim)

