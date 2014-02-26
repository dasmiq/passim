(ns passim
  (:require [clojure.string :as s]
            [clojure.set :as set]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.shell :as sh]
            [clojure.java.io :as jio]
            [ciir.utils :refer :all]
            [passim.galago :refer :all]
            [passim.utils :refer :all]
            [passim.quotes]
            [clojure.math.combinatorics :refer [combinations]])
  (:import (passim.utils Alignment)
           (org.lemurproject.galago.core.index IndexPartReader KeyIterator)
           (org.lemurproject.galago.core.index.disk DiskIndex)
           (org.lemurproject.galago.core.parse Document)
           (org.lemurproject.galago.core.retrieval Retrieval RetrievalFactory)
           (org.lemurproject.galago.tupleflow Parameters))
  (:gen-class))

(set! *warn-on-reflection* true)

(defn bill-doc
  [v]
  (str "<DOC>\n<DOCNO> " (second v) "/" (first v) " </DOCNO>\n<TEXT>\n"
       (s/replace (nth v 4) "\r\n" "\n")
       "</TEXT>\n</DOC>\n"))

(defn make-series-map
  [idx]
  (let [nidx (-> idx (jio/file "names.reverse") str)
        asize (inc (->> nidx dump-index (map #(Integer/parseInt (second %))) (reduce max)))
        res (make-array Integer/TYPE asize)]
    ;; (prn asize)
    (doseq [series (partition-by (comp doc-series first) (dump-index nidx))]
      (let [snum (Integer/parseInt (second (first series)))]
        (doseq [doc series]
          ;;(prn (Integer/parseInt (second doc)) snum)
          (aset-int res (Integer/parseInt (second doc)) snum))))
    #(get res %)))

(defn read-series-map
  [fname]
  (let [asize (-> (sh/sh "tail" "-1" fname) :out (s/split #"\t") first Integer/parseInt inc)
        res (make-array Integer/TYPE asize)]
    (with-open [in (jio/reader fname)]
      (doseq [line (line-seq in)]
        (let [[id series] (s/split line #"\t")]
          (aset-int res (Integer/parseInt id) (Integer/parseInt series)))))
    #(get res %)))

;; Sort by series, then multiply group sizes.
(defn cross-counts
  [series rec]
  (->> (nth rec 2)
       (map first)
       (map #(series %))
       frequencies
       vals
       (#(combinations % 2))
       (map (partial apply *))
       (reduce +)))

(defn cross-pairs
  [series upper max-df rec]
  (let [total-freq (second rec)]
    (when (<= total-freq upper)
      (let [k (first rec)
            npairs (cross-counts series rec)]
        (when (<= npairs upper)
          (let [docs (nth rec 2)]
            (for [[bid & brest] docs [aid & arest] docs
                  :while (< aid bid)
                  :when (and (not= (series aid) (series bid))
                             (<= (first arest) max-df)
                             (<= (first brest) max-df))]
              {[aid bid] [["" total-freq arest brest]]})))))))

(defn rand-blat
  [f prop coll]
  (filter
   #(do
      (when (<= (rand) prop)
        (binding [*out* *err*]
          (println (f %))))
      true)
   coll))

(defn merge-pairs
  "Merge postings for document pairs."
  [& argv]
  (let [[options remaining banner]
        (safe-cli argv
                  (str
                   "passim merge [options] <index>\n\n"
                   (var-doc #'merge-pairs))
                  ["-m" "--min-matches" "Minimum matching n-gram features" :default 1 :parse-fn #(Integer/parseInt %)]
                  ["-h" "--help" "Show help" :default false :flag true])
        {:keys [min-matches]} options]
    (doseq [recs (->> *in* jio/reader line-seq
                      (map edn/read-string) (partition-by ffirst))]
      (let [m (reduce (partial merge-with concat) {} recs)]
        (when (>= (-> m first second count) min-matches)
          (prn m))))))

(defn dump-pairs
  "Output document pairs with overlapping features."
  [& argv]
  (let [[options remaining banner]
        (safe-cli argv
                  (str
                   "passim pairs [options] <index>\n\n"
                   (var-doc #'dump-pairs))
                  ["-u" "--max-series" "Upper limit on effective series size" :default 100 :parse-fn #(Integer/parseInt %)]
                  ["-d" "--max-df" "Maximum document frequency in posting lists" :default 100 :parse-fn #(Integer/parseInt %)]
                  ["-m" "--series-map" "Map internal ids documents to integer series ids"]
                  ["-p" "--modp" "Keep only features whose hashes are divisible by p" :default 1 :parse-fn #(Integer/parseInt %)]
                  ["-r" "--modrec" "Keep only pairs whose hashes are divisible by r" :default 1 :parse-fn #(Integer/parseInt %)]
                  ["-s" "--step" "Chunk of index to read" :default 0 :parse-fn #(Integer/parseInt %)]
                  ["-t" "--stride" "Size of index chunks" :default 1000 :parse-fn #(Integer/parseInt %)]
                  ["-S" "--stop" "Stopword list"]
                  ["-h" "--help" "Show help" :default false :flag true])
        index-file ^String (first remaining)
        {:keys [series-map stop max-series max-df modp modrec step stride]} options]
    (let [ireader (DiskIndex/openIndexPart index-file)
          ki (.getIterator ireader)
          series (if series-map
                   (read-series-map series-map)
                   (make-series-map (.getParent (java.io.File. index-file))))
          stops (if stop (-> stop slurp (s/split #"\n") set (disj "")) #{})
          upper (/ (* max-series (dec max-series)) 2)]
      (dorun (repeatedly (* step stride) (fn [] (.nextKey ki))))
      ;; (println "#" step stride (.getKeyString ki))
      (doseq [item (cond->>
                    (->> ki dump-kl-index (take stride))
                    (> modp 1) (filter #(= 0 (mod (.hashCode ^String (first %)) modp)))
                    (not-empty stops) (remove #(some stops (s/split (first %) #"~")))
                    true (mapcat (partial cross-pairs series upper max-df))
                    (> modrec 1) (filter #(= 0 (mod (hash %) modrec))))]
        (prn item)))))

(defn- vappend
  [x y]
  (conj x (first y)))

(defn gap-postings
  [gap term-filter doc]
  (let [[id terms] doc
        len (count terms)]
    (into
     {}
     (map
      (fn [[k v]]
        (vector
         k [(vector id (count v) (vec v))]))
      (loop [i 0
             j (+ i (dec gap))
             posts {}]
        (if (>= j len)
          posts
          (recur (inc i)
                 (inc j)
                 (if (and (term-filter (terms i))
                          (term-filter (terms j)))
                   (merge-with vappend posts
                               {(str (terms i) "~" (terms j)) [i]})
                   posts))))))))

(defn- get-vocab
  [postings-file min-df max-df]
  (->> postings-file dump-index ;;(take 100000)
       (filter #(let [df (second %)]
                  (and (>= df min-df) (<= df max-df))))
       (map first)
       set))

(defn index-gaps
  [corpus-file postings-file gap min-df max-df step stride]
  (let [support (get-vocab postings-file min-df max-df)]
    (doseq [rec
            (->> corpus-file dump-corpus
                 (drop (* step stride)) (take stride)
                 (map (partial gap-postings gap support))
                 (reduce (partial merge-with vappend)))]
      (prn rec))))

(defn- read-match-data
  [s]
  (let [[ids data] (read-string (str "[" s "]"))]
    [ids (vec (partition 4 data))]))

(defn- spair
  [s]
  (vector (apply str (map first s)) (apply str (map second s))))

(defn word-substitutions
  [gram dict s1 s2]
  (let [target (- gram 2)]
    (->> (map vector (seq s1) (seq s2))
         (partition-by #{[\space \space]})
         (remove #{'([\space \space])})
         (map vec)
         (partition gram 1)
         (remove #(some #{\space} (flatten %)))
         (map #(map spair %))
         (filter
          (fn [x]
            (let [m (mapv (partial apply =) x)]
              (when (and (not (nth m target))
                         (= 1 (count (remove identity m))))
                ;;(when (and (first m) (second m) (not (nth m 2)) (nth m 3))
                (let [w1 (s/replace (first (nth x target)) "-" "")
                      w2 (s/replace (second (nth x target)) "-" "")
                      ] ;;diffs (remove (partial apply =) (map vector (seq (first (nth x 2))) (seq (second (nth x 2)))))]
                  (and
                   (> (count w1) 7)
                   (> (count w2) 7)
                   ;; Require edit distance > 1?
                   ;; (> (count diffs) 1)
                   ;; (not (prn diffs))
                   (dict w1)
                   (dict w2))))))))))

(defn hapax-positions
  "Returns map of hapax terms to their positions in sequence"
  [s]
  (->> s
       (map-indexed vector)
       (reduce
        (fn [map [pos word]]
          (update-in map [word] #(if (nil? %) pos -1)))
        {})
       (remove #(< (second %) 0))
       (into {})))

(defn find-hapax-anchors
  [terms1 terms2]
  (->>
   (merge-with vector (hapax-positions terms1) (hapax-positions terms2))
   vals
   (filter vector?)
   sort
   vec))

(defn- find-match-anchors
  [matches]
  (let [res (->> matches
                 (map
                  (fn [[gram df p1 p2]]
                    (when (= 1 (first p1) (first p2))
                      (vector (first (second p1)) (first (second p2))))))
                 (remove nil?)
                 sort
                 vec)]
    (when (not-empty res) res)))

(defn largest-binary-search
  [pred low high default]
  (if (< high low)
    default
    (let [mid (+ low (int (/ (- high low) 2)))]
      (cond (= mid low) (cond (pred high) high
                              (pred low) low
                              :else default)
            (pred mid) (recur pred mid high default)
            :else (recur pred low (dec mid) default)))))

(defn longest-increasing-subsequence
  "Output indices of the LIS"
  [coll]
  (when (seq coll)
    (let [s (vec coll)
          n (count s)]
      (loop [L 0
             i 1
             M (vec (take (inc n) (repeat 0)))
             P (vec (take (inc n) (repeat 0)))]
        (if (<= i n)
          (let [cur (s (dec i))
                j (largest-binary-search (fn [x] (< (s (dec (M (dec x)))) cur)) 1 L 0)
                newP (if (> j 0) (assoc P (dec i) (M (dec j))) P)]
            (if (or (= j L) (< cur (s (dec (M j)))))
              (recur (long (max L (inc j)))
                     (inc i)
                     (assoc M j i)
                     newP)
              (recur L
                     (inc i)
                     M
                     newP)))
          ;; Traceback
          (loop [res (list (dec (M (dec L))))]
            (let [prev (P (first res))]
              (if (> prev 0)
                (recur (cons (dec prev) res))
                (seq res)))))))))

(defn- swg-align
  [w1 w2]
  (let [c1 (join-alnum-tokens w1)
        c2 (join-alnum-tokens w2)
        alg (jaligner.SmithWatermanGotoh/align
             (jaligner.Sequence. c1) (jaligner.Sequence. c2) match-matrix 5 0.5)
        out1 (String. (.getSequence1 alg))
        out2 (String. (.getSequence2 alg))
        os1 (.getStart1 alg)
        os2 (.getStart2 alg)
        sword1 (+ (space-count (subs c1 0 os1))
                  (if (spacel? out1) 1 0))
        sword2 (+ (space-count (subs c2 0 os2))
                  (if (spacel? out2) 1 0))]
    (Alignment. out1 out2
                sword1 sword2
                    (+ sword1 1 (space-count (s/trim out1)))
                    (+ sword2 1 (space-count (s/trim out2))))))

(defn- align-words
  [start end w1 w2 gap-words]
  (let [[s1 s2] (if (not-empty start)
                  start
                  [(max 0 (- (first end) gap-words))
                   (max 0 (- (second end) gap-words))])
        [e1 e2] (if (not-empty end)
                  end
                  [(min (dec (count w1)) (+ (first start) gap-words))
                   (min (dec (count w2)) (+ (second start) gap-words))])
        c1 (join-alnum-tokens (subvec w1 s1 (inc e1)))
        c2 (join-alnum-tokens (subvec w2 s2 (inc e2)))
        alg (jaligner.SmithWatermanGotoh/align
             (jaligner.Sequence. c1) (jaligner.Sequence. c2) match-matrix 5 0.5)
        out1 (String. (.getSequence1 alg))
        out2 (String. (.getSequence2 alg))
        os1 (.getStart1 alg)
        os2 (.getStart2 alg)]
    ;; (prn [os1 os2 out1 out2])
    (when (and (or (empty? start) (= 0 os1 os2))
               ;; Probably should count alignment without hyphen gaps
               (or (empty? end) (and (<= (count c1) (+ os1 (count out1)))
                                     (<= (count c2) (+ os2 (count out2))))))
      (let [sword1 (+ s1 (space-count (subs c1 0 os1))
                      (if (spacel? out1) 1 0))
            sword2 (+ s2 (space-count (subs c2 0 os2))
                      (if (spacel? out2) 1 0))]
        (Alignment. out1 out2
                    sword1 sword2
                    (+ sword1 1 (space-count (s/trim out1)))
                    (+ sword2 1 (space-count (s/trim out2))))))))

(defn- word-offsets
  [words]
  (loop [pos (inc (count (first words)))
         offs [0]
         cur (rest words)]
    (if (empty? cur)
      offs
      (recur (+ pos 1 (count (first cur))) (conj offs pos) (rest cur)))))

(defn- increasing-matches
  [matches]
  (let [lis (longest-increasing-subsequence (map second matches))]
    (mapv (partial get matches) lis)))

(defn- trim-gram
  [^Alignment alg ^long gram]
  (let [m1 (dec gram)]
    (assoc alg
      :end1 (- (:end1 alg) m1)
      :end2 (- (:end2 alg) m1)
      :sequence1 (s/join " " (drop-last gram (s/split (:sequence1 alg) #" ")))
      :sequence2 (s/join " " (drop-last gram (s/split (:sequence2 alg) #" "))))))

(defn best-passages
  [w1 w2 matches gram]
  (when-let [anch (find-match-anchors matches)]
    (let [
          ;; (find-hapax-anchors (partition gram 1 w1) (partition gram 1 w2))
          ;; (->> matches vals first rest (map second) (map first) vec vector))
          inc-anch (increasing-matches anch)
          gap-words 100
          add-gram (partial + (dec gram))
          middles (mapcat
                   (fn [[[s1 s2] [e1 e2]]]
                     (if (> (max (- e1 s1) (- e2 s2)) 1)
                       (if (and (> (- e1 s1) gap-words) (> (- e2 s2) gap-words))
                         (list (align-words [s1 s2] [] w1 w2 gap-words)
                               nil
                               ;; shorten gap-words?
                               (align-words [] [e1 e2] w1 w2 gap-words))
                         (if-let [gap
                                  (align-words [s1 s2] [(add-gram e1) (add-gram e2)]
                                               w1 w2 gap-words)]
                           ;; Remove tacked-on trailing words
                           (list (trim-gram gap gram))
                           (list
                            (Alignment. (s/join " " (subvec w1 s1 (+ s1 gram)))
                                        (s/join " " (subvec w2 s2 (+ s2 gram)))
                                        s1 s2 (+ s1 gram) (+ s2 gram))
                            nil)))
                       (list (Alignment. (w1 s1) (w2 s2) s1 s2 (inc s1) (inc s2)))))
                   (partition 2 1 inc-anch))
          ;; removing trailing tacked-on words
          leading (when-let
                      [res (align-words [] (mapv add-gram (first inc-anch)) w1 w2 gap-words)]
                    (list (trim-gram res gram)))
          ;; Problem: not properly anchored at the left edge
          trailing (when-let
                       [res (align-words (nth inc-anch (dec (count inc-anch))) [] w1 w2 gap-words)]
                     (list res))]
      ;; (prn inc-anch)
      ;; (prn leading middles trailing)
      (remove nil?
              (for [span (partition-by nil? (concat leading middles trailing))]
                (when (first span)
                  (let [vspan (vec span)
                        head (first vspan)
                        tail (peek vspan)]
                    (Alignment.
                     (s/join " " (map (comp s/trim :sequence1) vspan))
                     (s/join " " (map (comp s/trim :sequence2) vspan))
                     (:start1 head) (:start2 head) (:end1 tail) (:end2 tail)))))))))

(defn score-pair
  [^String s ^Retrieval ri ^long gram]
  (let [[[id1 id2] matches] (first (edn/read-string s))
        name1 (.getDocumentName ri (int id1))
        name2 (.getDocumentName ri (int id2))
        words1 (doc-words ri name1)
        words2 (doc-words ri name2)
        approx-pass
        (try
          (if-let [passages (seq (best-passages words1 words2 matches
                                                (if (= gram 0) 1 gram)))]
            (reduce (maxer #(- (:end1 %) (:start1 %))) passages)
            (Alignment. "" "" 0 0 0 0))
          (catch Exception e
            (Alignment. "" "" 0 0 0 0))
          (catch OutOfMemoryError e
            (Alignment. "" "" 0 0 0 0)))
        pass (if (= gram 0)
               (try
                 (swg-align words1 words2)
                 (catch Exception e
                   (.println System/err e)
                   approx-pass)
                 (catch OutOfMemoryError e
                   (.println System/err e)
                   approx-pass))
               approx-pass)
        ;; nseries (count smeta)
        ;; idf (reduce +
        ;;             (map #(Math/log %)
        ;;                  (map (partial / nseries) (map first (vals matches)))))
        match-len1 (- (:end1 pass) (:start1 pass))
        match-len2 (- (:end2 pass) (:start2 pass))]
    (when (>= match-len1 gram)
      (s/join "\t" (concat [match-len1
                            (float (/ match-len1 (count words1)))
                            (float (/ match-len2 (count words2)))]
                           ((juxt :matches :gaps :swscore) (alignment-stats pass))
                           [id1 id2 name1 name2
                            (:start1 pass) (:end1 pass)
                            (:start2 pass) (:end2 pass)
                            (-> pass :sequence1 s/trim)
                            (-> pass :sequence2 s/trim)])))))

(defn load-series-meta
  [fname]
  (->> fname jio/reader line-seq
       (map #(let [fields (s/split % #"\t")]
               [(nth fields 3) (nth fields 2)]))
       (into {})))

(defn load-tab-map
  [fname]
  (->> fname jio/reader line-seq
       (map #(s/split % #"\t" 2))
       (into {})))

;; (def ri (RetrievalFactory/instance "/Users/dasmith/locca/ab/build/idx" (Parameters.)))
;; (def qwe (line-seq (jio/reader "/Users/dasmith/locca/ab/build/pairs/pall.1k")))

(defn dump-scores
  "Score document pairs based on n-gram overlap"
  [& argv]
  (let [[options remaining banner]
        (safe-cli argv
                  (str
                   "passim scores [options] <index>\n\n"
                   (var-doc #'dump-scores))
                  ["-n" "--ngram" "N-gram order" :default 5 :parse-fn #(Integer/parseInt %)]
                  ["-h" "--help" "Show help" :default false :flag true])
        idx ^String (first remaining)
        gram (:ngram options)]
  (let [ri (RetrievalFactory/instance idx (Parameters.))]
    (doseq [line (-> *in* jio/reader line-seq)]
      (when-let [out (score-pair line ri gram)]
        (println out))))))

(defn- vocab-set
  [s]
  (-> s (s/split #" ") set (disj "." "###")))

(defn jaccard
  [set1 set2]
  (/ (count (set/intersection set1 set2)) (count (set/union set1 set2))))

(defn complete-cluster-matches
  [m thresh voc id]
  (let [members (get-in m [:members id])]
    (= (count members)
       (count
        (filter
         #(> (jaccard voc (:vocabulary %)) thresh)
         members)))))

(defn single-cluster-matches
  [m thresh voc id]
  (let [members (get-in m [:members id])]
    (<= 1
        (count
         (filter
          #(> (jaccard voc (:vocabulary %)) thresh)
          members)))))

(defn absolute-overlap
  [rec1 rec2]
  (let [s1 ^long (:start rec1)
        e1 ^long (:end rec1)
        s2 ^long (:start rec2)
        e2 ^long (:end rec2)]
    (- (min e1 e2) (max s1 s2))))

(defn span-overlap
  [rec1 rec2]
  (let [s1 ^long (:start rec1)
        e1 ^long (:end rec1)
        s2 ^long (:start rec2)
        e2 ^long (:end rec2)
        len1 (double (- e1 s1))
        len2 (double (- e2 s2))
        shorter (double (min len1 len2))]
    (/ (max 0 (- (min e1 e2) (max s1 s2)))
       (max len1 len2))))

(defn single-link-matches
  [match-fn thresh m clusters1 clusters2 rec1 rec2]
  (let [id1 (:id rec1)
        id2 (:id rec2)]
    (set/union
     (set
      (filter #(>= (match-fn rec1 (get-in m [:members % id1])) thresh) clusters1))
     (set
      (filter #(>= (match-fn rec2 (get-in m [:members % id2])) thresh) clusters2)))))

(defn greedy-cluster-reducer
  [match-fn m line]
  (let [[sscore prop1 prop2 matches gaps ascore sid1 sid2 name1 name2 s1 e1 s2 e2 raw1 raw2]
        (s/split line #"\t")
        id1 (Integer/parseInt sid1)
        id2 (Integer/parseInt sid2)
        score (Double/parseDouble sscore)
        rec1 {:id id1 :name name1 :series (doc-series name1) :score score
              :start (Long/parseLong s1) :end (Long/parseLong e1) :text nil}
        rec2 {:id id2 :name name2 :series (doc-series name2) :score score
              :start (Long/parseLong s2) :end (Long/parseLong e2) :text nil}
        nextid (inc (get m :top 0))
        clusters1 (get-in m [:clusters id1] #{})
        clusters2 (get-in m [:clusters id2] #{})
        matches (match-fn m clusters1 clusters2 rec1 rec2)
        match (or (first matches) nextid)]
    (assoc
        (if (> (count matches) 1)
          (let [others (rest matches)
                orecs (map (partial get (:members m)) others)
                newrec (merge {id1 rec1 id2 rec2} (reduce merge orecs) (get-in m [:members match]))
                docs (keys newrec)
                newidx
                (into
                 {} (map
                     vector docs
                     (map #(conj (apply disj (get (:clusters m) % #{}) others) match) docs)))]
            ;; (println match "\t" others)
            ;; (println id1 "clusters1:" clusters1)
            ;; (println id2 "clusters2:" clusters2)
            ;; (println "docs:" docs)
            ;; (println "newidx:" newidx)
            ;; Need to dissociate the old cluster numbers from
            ;; *all* documents, not just id1 and id2
            ;; To test, take the first 877
            (-> m
                ;; need to dissoc old members entries
                (assoc :members (apply dissoc (:members m) others))
                (assoc-in [:members match] newrec)
                (assoc :clusters (merge (:clusters m) newidx))))
          (-> m
              (assoc-in [:members match] (merge {id1 rec1 id2 rec2} (get-in m [:members match])))
              (assoc-in [:clusters id1] (conj clusters1 match))
              (assoc-in [:clusters id2] (conj clusters2 match))))
      :top nextid)))

(defn- cluster-member-text
  [^Retrieval ri rec]
  (s/join " " (subvec (doc-words ri (:name rec)) (:start rec) (:end rec))))

(defn dump-cluster
  [cluster]
  [(->> cluster (map :name) set count)
   (mapv
    #((juxt :name :start :end) %)
    cluster)])

(defn top-rep-cluster
  [cluster]
  (->> cluster
       ;; NB: Not unique series, but same series with multiple IDs.
       (map (juxt :id :series))
       (into {})
       vals
       frequencies
       vals
       (reduce max)))

(defn cluster-scores
  "Single-link clustering of reprints"
  [& argv]
  (let [[options remaining banner]
        (safe-cli argv
                  (str
                   "passim cluster [options]\n\n"
                   (var-doc #'cluster-scores))
                  ["-m" "--min-overlap" "Minimum size of overlap" :default 0 :parse-fn #(Double/parseDouble %)]
                  ["-o" "--relative-overlap" "Proportion of longer text that must overlap" :default 0.5 :parse-fn #(Double/parseDouble %)]
                  ["-p" "--max-proportion" "Maximum proportion of cluster from one series" :default 1.0 :parse-fn #(Double/parseDouble %)]
                  ["-r" "--max-repeats" "Maximum number of texts from one series" :default 4 :parse-fn #(Integer/parseInt %)]
                  ["-h" "--help" "Show help" :default false :flag true])
        lines (-> *in* jio/reader line-seq)
        {:keys [min-overlap relative-overlap max-proportion max-repeats]} options]
    (doseq
        [cluster
         (->> lines
              (reduce (partial greedy-cluster-reducer
                               (if (> min-overlap 0)
                                 (partial single-link-matches absolute-overlap min-overlap)
                                 (partial single-link-matches span-overlap relative-overlap)))
                      {})
              :members
              vals
              (map vals)
              (remove
               (if (< max-proportion 1)
                 #(> (double (/ (top-rep-cluster %) (count %))) max-proportion)
                 #(> (top-rep-cluster %) max-repeats)))
              (map dump-cluster)
              (sort (comp - compare))
              (map-indexed
               #(hash-map :id (inc %1) :size (first %2) :members (second %2))))]
      (json/write cluster *out* :escape-slash false)
      (println))))

(defn loc-scale
  [in]
  (int (/ in 4)))

(defn loc-url
  [base text]
  (let [raw
        (->> text
             (re-seq #"<w p=\"([^\"]+)\" c=\"(\d+),(\d+),(\d+),(\d+)\"")
             (map (fn [[_ k & vals]] [k (mapv #(Integer/parseInt %) vals)])))
        p (ffirst raw)
        dat (map second (filter #(= p (first %)) raw))
        x1 (->> dat (map first) (reduce min) loc-scale)
        y1 (->> dat (map second) (reduce min) loc-scale)
        ;; What idiot put height before width in camun.pl?!?!?
        x2 (->> dat (map (fn [[x y h w]] (+ x w))) (reduce max) loc-scale)
        y2 (->> dat (map (fn [[x y h w]] (+ y h))) (reduce max) loc-scale)]
    (format "%s/%s/print/image_%dx%d_from_%d%%2C%d_to_%d%%2C%d/" base p
            600 600 x1 y1 x2 y2)))

(defn loc-words-url
  [base text]
  (let [raw
        (->> text
             (re-seq #"<w p=\"([^\"]+)\" c=\"[^\"]+\"/>(\S+)")
             (map rest))
        p (ffirst raw)
        words (->> raw (filter #(= p (first %))) (map #(s/replace (second %) #"[\.,!\?:;]$" "")) set)
        kwords (remove #(< (count %) 5) words)]
    (format "%s/%s/#words=%s" base p (s/join "+" kwords))))

(defn format-cluster
  "Format tab-separated cluster data"
  [& argv]
  (let [[options remaining banner]
        (safe-cli argv
                  (str
                   "passim format [options] <index>\n\n"
                   (var-doc #'format-cluster))
                  ["-h" "--help" "Show help" :default false :flag true])
        idx ^String (first remaining)
        lines (-> *in* jio/reader line-seq)
        ri (RetrievalFactory/instance idx (Parameters.))]
    (doseq [line lines]
      (let [{:keys [id size members]} (json/read-str line :key-fn keyword)]
        (doseq [[name start end] members]
          (let [m (doc-meta ri name)
                base-url (m "url")
                text (doc-text ri name start end)
                url (if (re-find #"<w p=" text)
                      (loc-words-url base-url text)
                      base-url)
                pretty-text
                (-> text
                    (s/replace #"</?[a-zA-Z][^>]*>" "")
                    (s/replace #"\n" "<br/>"))]
            (println
             (s/join "\t"
                     [id size (m "date")
                      (doc-series name)
                      (m "title")
                      url
                      start end pretty-text]))))))))

(defn gexf-lines
  [lines]
  (->> lines
       (mapcat
        (fn [line]
          (let [{:keys [id size members]} (json/read-str line :key-fn keyword)]
            (map
             #(vec (sort (map (comp doc-series first) %)))
             (combinations members 2)))))
       frequencies))

(defn gexf-cluster
  "Produce GEXF from cluster data for display by Gephi"
  [& argv]
  (let [[options remaining banner]
        (safe-cli argv
                  (str
                   "passim gexf [options] <index>\n\n"
                   (var-doc #'gexf-cluster))
                  ["-h" "--help" "Show help" :default false :flag true])
        idx ^String (first remaining)
        lines (-> *in* jio/reader line-seq)
        ri (RetrievalFactory/instance idx (Parameters.))
        bins (gexf-lines lines)]
    (println "<gexf>")
    (println "<graph defaultedgetype=\"undirected\">")
    (println "<nodes>")
    (doseq [n (set (mapcat first bins))]
      (printf
       "<node id=\"%s\" label=\"%s\" />\n"
       n n))
    (println "</nodes>\n<edges>")
    (doseq [[[s t] w] bins]
      (printf
       "<edge id=\"%s--%s\" source=\"%s\" target=\"%s\" weight=\"%d\" />\n"
       s t s t w))
    (println "</edges>\n</graph>\n</gexf>")))

(defn diff-words
  [gram lines]
  (let [dict (set (line-seq (jio/reader "/usr/share/dict/words")))]
    (doseq [line lines]
      (let [[sscore prop1 prop2 matches gaps ascore sid1 sid2 name1 name2 s1 e1 s2 e2 raw1 raw2]
            (s/split line #"\t")
            date1 (doc-date name1)
            date2 (doc-date name2)
            diffs (word-substitutions gram dict raw1 raw2)]
        (when (> (count diffs) 0)
          (doseq [diff diffs]
            (let [o1 (s/join " " (map first diff))
                  o2 (s/join " " (map second diff))]
              (println
               (s/join
                "\t"
                (if (< (compare date1 date2) 0)
                  [sscore date1 date2 o1 o2 name1 name2]
                  [sscore date2 date1 o2 o1 name2 name1]))))))))))

(defn -main
  "Usage: passim command [command-options]"
  [& argv]
  (let [commands
        {"pairs" #'dump-pairs
         "merge" #'merge-pairs
         "scores" #'dump-scores
         "cluster" #'cluster-scores
         "format" #'format-cluster
         "gexf" #'gexf-cluster
         "quotes" #'passim.quotes/dump-quotes}
        usage
        (str
         (var-doc #'-main)
         "\n\nCommands:\n"
         (s/join
          "\n"
          (map
           (fn [[k v]]
             (str "\t" k "\t\t" (var-doc v)))
           commands)))]
    (if (seq argv)
      (let [[cmd & args] argv]
        (if-let [v (commands cmd)]
          (apply v args)
          (exit 1 usage)))
      (exit 1 usage))))

      ;;   (condp = cmd
      ;;     "diffs" (diff-words
      ;;              (Long/parseLong (first args))
      ;;              (-> System/in java.io.InputStreamReader. java.io.BufferedReader. line-seq))
      ;;     ;; These are for debugging and aren't used much.
      ;;     "gaps" (index-gaps (first args) (second args)
      ;;                        (Integer/parseInt (nth args 2)) (Integer/parseInt (nth args 3))
      ;;                        (Integer/parseInt (nth args 4)) (Integer/parseInt (nth args 5))
      ;;                        (Integer/parseInt (nth args 6)))                       
      ;;     "counts" (->> (first args) dump-index (map second) frequencies prn)
      ;;     "entries"  (->> (first args) dump-index count prn)
      ;;     "total"  (->> (first args) dump-index (rand-blat first 0.001) (map second) (reduce +) prn)
      ;;     "dump" (doseq
      ;;                [s (->> (first args) dump-index)]
      ;;              (println s))
      ;;     "easy-dump" (kv-dump (DiskIndex/openIndexPart (first args)))
      ;;     (exit 1 usage)
      ;; (exit 1 usage))))))
