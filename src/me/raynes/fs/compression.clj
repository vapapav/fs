(ns me.raynes.fs.compression
  "Compression utilities."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [me.raynes.fs :as fs])
  (:import (java.util.zip ZipFile GZIPInputStream)
           (org.apache.commons.compress.archivers.tar TarArchiveInputStream
                                                      TarArchiveOutputStream
                                                      TarArchiveEntry)
           (org.apache.commons.compress.compressors bzip2.BZip2CompressorInputStream
                                                    bzip2.BZip2CompressorOutputStream
                                                    xz.XZCompressorInputStream
                                                    xz.XZCompressorOutputStream)
           (java.io FileOutputStream BufferedOutputStream ByteArrayOutputStream)))

(defn unzip
  "Takes the path to a zipfile `source` and unzips it to target-dir."
  ([source]
     (unzip source (name source)))
  ([source target-dir]
     (with-open [zip (ZipFile. (fs/file source))]
       (let [entries (enumeration-seq (.entries zip))
             target-file #(fs/file target-dir (str %))]
         (doseq [entry entries :when (not (.isDirectory ^java.util.zip.ZipEntry entry))
                 :let [f (target-file entry)]]
           (fs/mkdirs (fs/parent f))
           (io/copy (.getInputStream zip entry) f))))
     target-dir))

(defn- add-zip-entry
  "Add a zip entry. Works for strings and byte-arrays."
  [^java.util.zip.ZipOutputStream zip-output-stream [^String name content & remain]]
  (.putNextEntry zip-output-stream (java.util.zip.ZipEntry. name))
  (if (string? content) ;string and byte-array must have different methods
    (doto (java.io.PrintStream. zip-output-stream true)
      (.print content))
    (.write zip-output-stream ^bytes content))
  (.closeEntry zip-output-stream)
  (when (seq (drop 1 remain))
    (recur zip-output-stream remain)))

(defn make-zip-stream
  "Create zip file(s) stream. You must provide a vector of the
  following form: 

  ```[[filename1 content1][filename2 content2]...]```.

  You can provide either strings or byte-arrays as content.

  The piped streams are used to create content on the fly, which means
  this can be used to make compressed files without even writing them
  to disk."
  [& filename-content-pairs]
  (let [file
    (let [pipe-in (java.io.PipedInputStream.)
          pipe-out (java.io.PipedOutputStream. pipe-in)]
      (future
        (with-open [zip (java.util.zip.ZipOutputStream. pipe-out)]
          (add-zip-entry zip (flatten filename-content-pairs))))
      pipe-in)]
    (io/input-stream file)))

(defn zip
  "Create zip file(s) on the fly. You must provide a vector of the
  following form: 

  ```[[filename1 content1][filename2 content2]...]```.

  You can provide either strings or byte-arrays as content."
  [filename & filename-content-pairs]
  (io/copy (make-zip-stream filename-content-pairs)
           (fs/file filename)))

(defn- slurp-bytes [fpath]
  (with-open [data (io/input-stream (fs/file fpath))]
    (with-open [out (ByteArrayOutputStream.)]
      (io/copy data out)
      (.toByteArray out))))

(defn make-zip-stream-from-files
  "Like make-zip-stream but takes a sequential of file paths and builds filename-content-pairs
   based on those"
  [fpaths]
  (let [filename-content-pairs (map (juxt fs/base-name slurp-bytes) fpaths)]
    (make-zip-stream filename-content-pairs)))

(defn zip-files
  "Zip files provided in argument vector to a single zip. Converts the argument list:

  ```(fpath1 fpath2...)```

  into filename-content -pairs, using the original file's basename as the filename in zip`and slurping the content:

  ```([fpath1-basename fpath1-content] [fpath2-basename fpath2-content]...)``"
  [filename fpaths]
  (io/copy (make-zip-stream-from-files fpaths)
           (fs/file filename)))

;; begin tar

(defn- tar-entries
  "Get a lazy-seq of entries in a tarfile."
  [^TarArchiveInputStream tin]
  (when-let [entry (.getNextTarEntry tin)]
    (cons entry (lazy-seq (tar-entries tin)))))

(defn untar
  "Takes a tarfile `source` and untars it to `target`."
  ([source] (untar source (name source)))
  ([source target]
     (with-open [tin (TarArchiveInputStream. (io/input-stream (fs/file source)))]
       #_(log/debug source "has" (count (tar-entries tin)) "entries")
       (doseq [^TarArchiveEntry entry (tar-entries tin) :when (not (.isDirectory entry))
               :let [output-file (fs/file target (.getName entry))]]
         (log/debug "Making parent directories for" (.getAbsolutePath output-file))
         (fs/mkdirs (fs/parent output-file))
         (io/copy tin output-file)
         (log/debug "Copied tin to" (.getAbsolutePath output-file))
         (when (.isFile entry)
           (fs/chmod (apply str (take-last
                                 3 (format "%05o" (.getMode entry))))
                     (.getPath output-file)))))))

(defn- add-tar-entry
  "Add a single tar entry for the given file."
  [^TarArchiveOutputStream tar-output-stream ^String fpath]
  (let [f (io/file fpath)]
    (log/debug "Add" fpath "with name" (.getName f) "to archive")
    (.putArchiveEntry tar-output-stream (.createArchiveEntry tar-output-stream f (.getName f)))
    (io/copy (io/input-stream f) tar-output-stream)
    (.closeArchiveEntry tar-output-stream)))

(defn- compression-stream [out compression-format]
  (case compression-format
    :bz2 (BZip2CompressorOutputStream. out)
    :xz (XZCompressorOutputStream. out)
    out))

(defn tar-files
  "Create tar archive with given base name out of given files, with optional compression format.
   Returns name of archive, after tar and compression format are appended."
  [filename-base fpaths & {:keys [compression-format] :or {compression-format :bz2}}]
  (let [filename (str filename-base ".tar." (name compression-format))]
    (with-open [file-out (java.io.FileOutputStream. filename)
                buff-out (BufferedOutputStream. file-out)
                comp-out (compression-stream buff-out compression-format)
                tar-out (TarArchiveOutputStream. comp-out)]
      (run! #(add-tar-entry tar-out %) fpaths))
    (io/file filename)))

;; end tar

(defn gunzip
  "Takes a path to a gzip file `source` and unzips it."
  ([source] (gunzip source (name source)))
  ([source target]
     (io/copy (-> source fs/file io/input-stream GZIPInputStream.)
              (fs/file target))))

(defn bunzip2
  "Takes a path to a bzip2 file `source` and uncompresses it."
  ([source] (bunzip2 source (name source)))
  ([source target]
     (io/copy (-> source fs/file io/input-stream BZip2CompressorInputStream.)
              (fs/file target))))

(defn unxz
  "Takes a path to a xz file `source` and uncompresses it."
  ([source] (unxz source (name source)))
  ([source target]
    (io/copy (-> source fs/file io/input-stream XZCompressorInputStream.)
             (fs/file target))))
