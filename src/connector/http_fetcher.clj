(ns connector.http-fetcher
  "Downloader for cloud sources (no manual CSV cleaning)."
  (:require [clojure.java.io :as io]
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
              (when-let [m (re-find pat url)]
                (second m)))
            [#"/file/d/([a-zA-Z0-9_-]+)"
             #"[?&]id=([a-zA-Z0-9_-]+)"
             #"/open\\?id=([a-zA-Z0-9_-]+)"])
      (throw (ex-info "Cannot extract Drive file ID"
                      {:url url}))))

(defn- force-dropbox-dl
  [link]
  (cond
    ;; replace preview mode
    (str/includes? link "dl=")
    (str/replace link #"dl=0" "dl=1")

    ;; append param
    (str/includes? link "?")
    (str link "&dl=1")

    :else
    (str link "?dl=1")))

(defn build-gdrive-request
  [link revision-id]

  (let [id (extract-gdrive-id link)]

    {:url (if revision-id
            (str "https://www.googleapis.com/drive/v3/files/"
                 id
                 "/revisions/"
                 revision-id
                 "?alt=media")

            (str "https://www.googleapis.com/drive/v3/files/"
                 id
                 "?alt=media"))
     :method :get
     :headers {}}))

(defn build-dropbox-request
  [link token revision-id]
  (cond
    ;; revision download
    revision-id
    {:url "https://content.dropboxapi.com/2/files/download"
     :method :post
     :headers {"Dropbox-API-Arg"
               (str "{\"path\":\"rev:" revision-id "\"}")}}

    ;; private file latest version
    (not (str/blank? token))
    {:url "https://content.dropboxapi.com/2/files/download"
     :method :post
     :headers {"Dropbox-API-Arg"
               (str "{\"path\":\"" link "\"}")}}

    ;; public share link
    :else
    {:url (force-dropbox-dl link)
     :method :get
     :headers {}}))

;;;; main downloader

(defn fetch-file!
  [type {:keys [link token revision-id]}]
  (let [{:keys [url method headers]}
        (case type
          :gdrive (build-gdrive-request link revision-id)

          :dropbox (build-dropbox-request link token revision-id)

          {:url link
           :method :get
           :headers {}})
        
        dest (str (System/getProperty "java.io.tmpdir")
                  File/separator
                  "cloud-raw-"
                  (UUID/randomUUID)
                  ".csv")]
    (log/info
     {:msg "starting cloud download"
      :metric {:type type :url url :dest dest}})
    (let [req-builder (HttpRequest/newBuilder)]
      (.uri req-builder (URI/create url))
      (.timeout req-builder
                (Duration/ofSeconds 120))
      ;; common auth header (works for gdrive + dropbox)
      (when (not (str/blank? token))
        (.header req-builder
                 "Authorization"
                 (str "Bearer " token)))
      ;; provider specific headers
      (doseq [[k v] headers]
        (.header req-builder k v))
      (let [request (if (= method :post)
                      (.build
                       (.POST
                        req-builder
                        (java.net.http.HttpRequest$BodyPublishers/noBody)))
                      (.build
                       (.GET req-builder)))
            response (.send
                      http-client
                      request
                      (HttpResponse$BodyHandlers/ofInputStream))

            status (.statusCode
                    response)]
        (when (or (< status 200) (>= status 300))
          (throw (ex-info
                  "Cloud download failed"
                  {:status status
                   :url url
                   :type type})))

        (with-open [in (.body response)
                    out (io/output-stream dest)]

          (io/copy in out))))

    (.deleteOnExit (File. dest))

    dest))