(ns connector.gdrive
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io  :as io]
   [clojure.string   :as str]
   [taoensso.timbre  :as log])
  (:import
   [java.io File]
   [java.net URI]
   [java.net.http HttpClient
                  HttpClient$Redirect
                  HttpRequest
                  HttpResponse$BodyHandlers]
   [java.time Duration]
   [java.util UUID]))


(def ^:private ^HttpClient http-client
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 30))
      (.followRedirects HttpClient$Redirect/NORMAL)
      (.build)))


(def ^:private file-id-patterns
  [#"/file/d/([a-zA-Z0-9_-]+)"
   #"[?&]id=([a-zA-Z0-9_-]+)"
   #"/open\?id=([a-zA-Z0-9_-]+)"])

(defn- extract-file-id
  [url]
  (or (some (fn [pat]
              (when-let [m (re-find pat url)]
                (second m)))
            file-id-patterns)
      (throw (ex-info "Cannot extract Drive file ID from URL"
                      {:url url}))))

(defn- resolve-version
  [v]
  (let [resolved (cond
                   (nil? v)     :v3
                   (keyword? v) v
                   (string? v)  (keyword v)
                   :else        :v3)]
    (when-not (#{:v2 :v3} resolved)
      (throw (ex-info "Unsupported Google Drive API version"
                      {:version   resolved
                       :supported [:v3 :v2]})))
    resolved))

(defmulti ^:private build-download-url
  (fn [version _file-id] version))

(defmethod build-download-url :v3
  [_ file-id]
  (str "https://www.googleapis.com/drive/v3/files/" file-id "?alt=media"))

(defmethod build-download-url :v2
  [_ file-id]
  (str "https://www.googleapis.com/drive/v2/files/" file-id "?alt=media"))

(defn- build-request 
  ^HttpRequest [url-str token]
  (-> (HttpRequest/newBuilder)
      (.uri (URI/create url-str))
      (.timeout (Duration/ofSeconds 120))
      (.header "Authorization" (str "Bearer " token))
      (.GET)
      (.build)))

(defn- check-status!
  [response url]
  (let [status (.statusCode response)]
    (when (or (< status 200) (>= status 300))
      (throw (ex-info "Drive download request failed"
                      {:status status
                       :url    url})))))

(defn- strip-bom
  "Removes UTF-8 BOM (\\uFEFF) from the start of a string if present."
  [s]
  (if (and (seq s) (= (first s) \uFEFF))
    (subs s 1)
    s))

(defn- clean-row
  "Trims leading/trailing whitespace from every cell in a row."
  [row]
  (mapv str/trim row))

(defn- blank-row?
  "Returns true when every cell in the row is blank after trimming."
  [row]
  (every? str/blank? row))

(defn- clean-header
  "Strips BOM from the first header cell and trims all header cells."
  [header]
  (-> header
      (update 0 strip-bom)
      clean-row))

(defn- temp-path
  "Returns a unique temp file path under the system temp directory."
  []
  (str (System/getProperty "java.io.tmpdir")
       File/separator
       "gdrive-clean-" (UUID/randomUUID) ".csv"))


(defn- stream-clean-to-file!
  [input-stream dest-path {:keys [delimiter quote-char]
                            :or   {delimiter  ","
                                   quote-char \"}}]
  (let [sep (first delimiter)
        qc  (if (char? quote-char)
              quote-char
              (first (str quote-char)))]
    (with-open [rdr (io/reader input-stream)
                wtr (io/writer dest-path)]
      (let [[hdr & rows] (csv/read-csv rdr :separator sep :quote qc)
            clean-hdr    (clean-header hdr)
            clean-rows   (->> rows
                              (remove blank-row?)
                              (map clean-row))]
        ;; csv/write-csv consumes the lazy seq row-by-row.
        ;; The full dataset is never realised into memory.
        (csv/write-csv wtr
                       (cons clean-hdr clean-rows)
                       :separator sep
                       :quote qc)))))

(defn fetch-and-clean!
  [{:keys [link token version]} options]
  (let [file-id  (extract-file-id link)
        ver      (resolve-version version)
        url      (build-download-url ver file-id)
        dest     (temp-path)]

    (log/info {:msg    "starting Drive stream-download + clean"
               :metric {:file-id file-id
                        :version ver
                        :url     url
                        :dest    dest}})

    (let [request  (build-request url token)
          response (.send http-client
                          request
                          (HttpResponse$BodyHandlers/ofInputStream))]

      (check-status! response url)
      (with-open [body-stream (.body response)]
        (stream-clean-to-file! body-stream dest options)))
    (.deleteOnExit (File. dest))
    (log/info {:msg    "Drive stream-download + clean complete"
               :metric {:dest dest}})
    dest))