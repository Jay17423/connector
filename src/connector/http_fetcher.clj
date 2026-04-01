(ns connector.http-fetcher
  "Generalized downloader for cloud sources with streaming clean logic."
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.io File]
           [java.net URI]
           [java.net.http
            HttpClient
            HttpClient$Redirect
            HttpRequest
            HttpResponse$BodyHandlers]
           [java.time Duration]
           [java.util UUID]))

(def http-client
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 30))
      (.followRedirects HttpClient$Redirect/NORMAL)
      (.build)))

(defn- extract-gdrive-id [url]
  (or (some (fn [pat]
              (when-let [m (re-find pat url)] (second m)))
            [#"/file/d/([a-zA-Z0-9_-]+)"
             #"[?&]id=([a-zA-Z0-9_-]+)"
             #"/open\\?id=([a-zA-Z0-9_-]+)"])
      (throw (ex-info "Cannot extract Drive file ID" {:url url}))))

(defn build-url
  "Transforms user link into a direct download stream link based on source 
   type."
  [type link revision-id]
  (case type
    :gdrive (let [id (extract-gdrive-id link)]
              (if revision-id
                (str "https://www.googleapis.com/drive/v3/files/" 
                     id "/revisions/" 
                     revision-id "?alt=media")
                (str "https://www.googleapis.com/drive/v3/files/" 
                     id "?alt=media")))
    :dropbox (if (str/includes? link "?")
               (str/replace link #"\?dl=0" "?dl=1")
               (str link "?dl=1"))
    link)) ;; S3/GCS/Others use the link as-is

(defn stream-clean-to-file!
  "Streams CSV, removes BOM + empty rows and writes to local temp file."
  [input-stream dest-path {:keys [delimiter quote-char]
                           :or {delimiter "," quote-char \"}}]
  (let [sep (first delimiter)
        qc (if (char? quote-char)
             quote-char
             (first (str quote-char)))]
    (with-open [rdr (io/reader input-stream)
                wtr (io/writer dest-path)]
      (let [[hdr & rows] (csv/read-csv rdr :separator sep :quote qc)
            header (when hdr
                     (assoc hdr 0
                            (let [v (first hdr)]
                              (if (and v (= (first v) \uFEFF))
                                (subs v 1)
                                v))))
            clean-rows (remove (fn [row]
                                 (every?
                                  (fn [col]
                                    (str/blank? (or col "")))
                                  row))
                               rows)]
        (csv/write-csv wtr
                       (cons header clean-rows)
                       :separator sep :quote qc)))))

(defn fetch-and-clean!
  "Generalized fetcher for all cloud sources."
  [type {:keys [link token revision-id]} options]
  (let [url (build-url type link revision-id)
        dest (str (System/getProperty "java.io.tmpdir")
                  File/separator "cloud-clean-" (UUID/randomUUID) ".csv")]
    
    (log/info {:msg "starting cloud stream-download + clean" 
               :metric {:type type :url url :dest dest}})
    
    (let [req-builder (HttpRequest/newBuilder)
          _ (.uri req-builder (URI/create url))
          _ (.timeout req-builder (Duration/ofSeconds 120))
          _ (when (not (str/blank? token))
              (.header 
               req-builder "Authorization" (str "Bearer " token)))
          request (.build
                   (.GET req-builder))
          response (.send http-client
                          request
                          (HttpResponse$BodyHandlers/ofInputStream))
          status (.statusCode response)]
      (when (or (< status 200) (>= status 300))
        (throw (ex-info
                "Cloud download request failed"
                {:status status :url url :type type})))
      (with-open [body-stream (.body response)]
        (stream-clean-to-file! body-stream dest options)))
    (.deleteOnExit (File. dest))
    dest))