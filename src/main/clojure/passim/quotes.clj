(ns passim.quotes
  (:require [clojure.string :as s]
            [clojure.java.io :as jio]
            [clojure.data.json :as json]
            [ciir.utils :refer :all]
            [passim.utils :refer :all]
            [passim.galago :refer :all])
  (:import (passim.utils Alignment)
           (org.lemurproject.galago.core.index IndexPartReader KeyIterator)
           (org.lemurproject.galago.core.index.disk DiskIndex)
           (org.lemurproject.galago.core.parse Document)
           (org.lemurproject.galago.core.retrieval Retrieval RetrievalFactory)
           (org.lemurproject.galago.tupleflow Parameters Utility)))

(defn- index-positions
  "Returns map of terms to their positions in sequence"
  [s]
  (->> s
       (map-indexed vector)
       (reduce
        (fn [map [pos word]]
          (merge-with (comp vec concat) map {word [pos]}))
        {})
       (into {})))

(defn- index-tokens
  [docs gram]
  (let [names (mapv first docs)
        toks (map (comp galago-tokenize second) docs)
        idx (apply concat (map-indexed (fn [pos words] (map #(vector % pos) words)) toks))
        words (mapv first idx)]
    {:names names
     :positions (mapv second idx)
     :words words
     :terms (->> words
                 (partition gram 1)
                 (map #(s/join "~" %))
                 vec)}))

(defn- term-hits
  [^KeyIterator ki max-count bad-docs terms]
  (.reset ki)
  (reduce
   (fn [m term]
     (.skipToKey ki (Utility/fromString term))
     (if (= (.getKeyString ki) term)
       (let [vi (.getValueIterator ki)]
         (if (<= (.totalEntries vi) max-count)
           (assoc m term (vec (remove bad-docs (map first (value-iterator-seq vi)))))
           m))
       m))
   {}
   (sort terms)))

(defn load-tsv
  [fname]
  (map #(s/split % #"\t") (s/split (slurp fname) #"\n")))

(defn- doc-passage
  [^Document d start end]
  (let [[id n] (doc-id-parts (.name d))
        len (count (.terms d))
        soff (if (> start 0)
               (+ 5 (.get (.termCharEnd d) (dec start)))
               0)
        eoff (+ 4 (.get (.termCharEnd d) (dec end)))
        raw (subs (.text d) soff eoff)
        coords (re-seq #" coords=\"([0-9]+),([0-9]+),([0-9]+),([0-9]+)" raw)
        x (->> coords (map #(Integer/parseInt (nth % 1))) (reduce min))
        y (->> coords (map #(Integer/parseInt (nth % 4))) (reduce min))
        w (- (->> coords (map #(Integer/parseInt (nth % 3))) (reduce max)) x)
        h (- (->> coords (map #(Integer/parseInt (nth % 2))) (reduce max)) y)]
    {:id id
     :p n
     :text2
     (-> raw
         (s/replace #"<lb>" "\n")
         (s/replace #"</?[A-Za-z][^>]*>" ""))
     :bbox
     [x y w h]
     :url
     (str "http://www.archive.org/download/" id "/page/n" (dec (Integer/parseInt n))
          (format "_x%d_y%d_w%d_h%d.jpg" x y w h))
     }))

(defn- proc-aligned-doc
  [out1 out2 idx sword1 eword1 ^Document doc sword2 eword2]
  (let [[id n] (doc-id-parts (.name doc))
        w1 (:words idx)
        raw (.text doc)
        terms (.terms doc)
        tce (.termCharEnd doc)]
    ;;(println (s/join " " (subvec w1 sword1 eword1)))
    (loop [res []
           c1 (seq (s/trimr out1))
           c2 (seq (s/trimr out2))
           s1 sword1
           s2 sword2]
      (if (and (not-empty c1) (not-empty c2))
        (let [f1 (first c1)
              f2 (first c2)]
          (recur
           ;; Maybe we should record words even when just the
           ;; canonical text has a word break. This would
           ;; allow... There is a bug below, which I think ultimately
           ;; comes from the book indexing code: galago tokenizes too
           ;; much.
           (if (and (= \space f1) (= \space f2))
             (let [soff (if (> s2 0)
                          (+ 5 (.get tce (dec s2)))
                          0)
                   eoff (+ 4 (.get tce s2))
                   raw (subs (.text doc) soff eoff)
                   coords (re-seq #" coords=\"([0-9]+),([0-9]+),([0-9]+),([0-9]+)" raw)
                   x1 (->> coords (map #(Integer/parseInt (nth % 1))) (reduce min 1000))
                   y1 (->> coords (map #(Integer/parseInt (nth % 4))) (reduce min 1000))
                   x2 (->> coords (map #(Integer/parseInt (nth % 3))) (reduce max 0))
                   y2 (->> coords (map #(Integer/parseInt (nth % 2))) (reduce max 0))]
               (conj res
                     {:id id
                      :p n
                      :s1 s1
                      :s2 s2
                      :w1 (w1 s1)
                      :w2 (.get terms s2)
                      :bbox [x1 y1 x2 y2]
                      :cite (-> s1 ((:positions idx)) ((:names idx)))}))
             res)
           (rest c1)
           (rest c2)
           (if (= \space f1) (inc s1) s1)
           (if (= \space f2) (inc s2) s2)))
        res))))

;; We should include the canonical texts themselves in the index so
;; that their ngrams show up as occurring at least once.  We should
;; therefore also remove hits to these texts from the results below.
(defn quoted-passages
  [docs gram bad-docs ^KeyIterator ki ^Retrieval ri]
  (let [max-count 1000
        max-gap 200
        idx (index-tokens docs gram)
        term-pos (index-positions (:terms idx))
        term-count (count (:terms idx))
        page-hits
        (->> term-pos
             keys
             (term-hits ki max-count bad-docs)
             (reduce
              (fn [m [t d]]
                (let [tf (count d)
                      pos (mapv #(vector % tf) (term-pos t))]
                  (merge-with
                   (comp vec concat) m
                   (into {} (map #(vector % pos) d)))))
              {})
             (map
              (fn [[k v]]
                [(.getDocumentName ri (int k)) (vec (sort v))])))
        book-hits (frequencies (map (comp first doc-id-parts first) page-hits))
        hits (->> page-hits
             (map
              (fn [[page thits]]
                (let [matches
                      (map #(mapv first %)
                           (partition-when
                            (fn [[[s _] [e _]]] (> (- e s) max-gap))
                            (partition 2 1 [[-1 0]] thits)))]
                  [page
                   (map #(let [pos (mapv first %)]
                           [(first pos) (peek pos)
                            ;;(->> % (map second) count)
                            (->> % (map second) (map (fn [x] (Math/log (inc (/ 1 x))))) (reduce +))
                            ])
                        matches)])))
             sort
             ;; We keep a single record for each page, with multiple
             ;; spans, so we can save time and look up the text for a
             ;; page once.
             (mapcat
              (fn [[page spans]]
                (let [pterms (doc-words ri page)
                      doc-data (get-index-doc ri page)
                      c2 (join-alnum-tokens pterms)
                      pseq (jaligner.Sequence. c2)]
                  (map (fn [[s e score]]
                         (let [s1 (max 0 (- s 50))
                               c1 (join-alnum-tokens
                                   (subvec (:words idx)
                                           s1
                                           (min (dec term-count) (+ e 50))))
                               alg (jaligner.SmithWatermanGotoh/align
                                    (jaligner.Sequence. c1)
                                    pseq
                                    match-matrix 5 0.5)
                               out1 (String. (.getSequence1 alg))
                               out2 (String. (.getSequence2 alg))
                               os1 (.getStart1 alg)
                               os2 (.getStart2 alg)
                               sword1 (+ s1 (space-count (subs c1 0 os1))
                                         (if (spacel? out1) 1 0))
                               sword2 (+ 0 (space-count (subs c2 0 os2))
                                         (if (spacel? out2) 1 0))
                               eword1 (+ sword1 1 (space-count (s/trim out1)))
                               eword2 (+ sword2 1 (space-count (s/trim out2)))]
                           (merge
                            (doc-passage doc-data sword2 eword2)
                            (alignment-stats (Alignment. out1 out2 sword1 sword2 eword1 eword2))
                            {:text1 (s/join " " (subvec (:words idx) sword1 eword1))
                             :score score
                             :cites
                             (mapv #(get (:names idx) %) (distinct (subvec (:positions idx) sword1 eword1)))
                             :words
                             (proc-aligned-doc
                              out1 out2 idx sword1 eword1 doc-data sword2 eword2)
                             :page page})))
                       spans)))))]
    hits))

(defn dump-quotes
  "Find passages in a reference text that align well to passages using an an n-gram index."
  [& argv]
  (let [[options remaining banner]
        (safe-cli argv
                  (str
                   "passim quotes [options] <n-gram index> (<reference text file> | -)+\n\n"
                   (var-doc #'dump-quotes))
                  ["-p" "--pretty" "Pretty-print JSON output" :default false :flag true]
                  ["-h" "--help" "Show help" :default false :flag true])]
    (try
      (let [[idx & tfiles] remaining
            printer (if (:pretty options)
                      #(json/pprint % :escape-slash false)
                      #(json/write % *out* :escape-slash false))
            dir (.getParent (java.io.File. idx))
            bad-docs (->> (jio/file dir "names") str dump-index
                          (filter #(re-find #"^urn:cts:" (second %)))
                          (map #(Long/parseLong (first %)))
                          set)
            di (DiskIndex/openIndexPart idx)
            gram (.get (.getManifest di) "n" 5)
            ki (.getIterator di)
            ri (RetrievalFactory/instance dir (Parameters.))]
        (doseq [f (if (seq tfiles) tfiles ["-"])
                q (-> (if (= "-" f) *in* f)
                      load-tsv
                      (quoted-passages gram bad-docs ki ri))]
          (printer q)
          (println)))
      (catch Exception e
        (println e)
        (exit 1 banner)))))
