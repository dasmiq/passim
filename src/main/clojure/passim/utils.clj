(ns passim.utils
  (:require [clojure.string :as s]))

(defn doc-id-parts
  [docid]
  (s/split docid #"[_/]" 2))

(defn doc-series
  [docid]
  (first (doc-id-parts docid)))

(defn doc-date
  [docid]
  (second (doc-id-parts docid)))

(defn doc-series-pair
  [[no id]]
  [(Integer/parseInt no) (doc-series id)])

(defn space-count
  [^String s]
  (count (re-seq #" " s)))

(defn spacel?
  [^String s]
  (= " " (subs s 0 1)))

(defn join-alnum-tokens
  [toks]
  (s/replace (s/join " " toks) #"[^a-zA-Z0-9 ]" "#"))

(def match-matrix (jaligner.matrix.MatrixGenerator/generate 2 -1))

