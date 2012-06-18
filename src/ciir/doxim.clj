(ns ciir.doxim
  (:require [clojure.string :as s]
            [clojure.java.io :as jio])
  (:use [clojure.math.combinatorics])
  (:import (org.lemurproject.galago.core.index disk.DiskIndex IndexPartReader KeyIterator))
  (:gen-class))

(defn -main
  "I don't do a whole lot."
  [& args]
  (println "Hello, World!"))
