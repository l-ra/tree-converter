package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func workerCount() int {
	return 20
}

/*
======================================================================================
*/
type PdfInfo struct {
	filename string
	pages    int
	size     int
}

/*
======================================================================================
*/
type Computation struct {
	files chan string
	//pdfInfo        chan PdfInfo
	PageCount      uint64
	imageCount     uint64
	TotalInputSize uint64
	srcDir         string
	dstDir         string
	userPasswd     string
	deleteOutput   bool
	filePattern    string
	fileCount      uint64
	matchedCount   uint64
	finishedCount  uint64
	tasks          sync.WaitGroup
	skipPdfInfo    bool
	skipPdfConvert bool
	skipListImages bool
	skipToText     bool
	skipLockFile   bool
	fileInfos      map[string]*FileInfo
	fileInfosLock  sync.RWMutex
}

type FileInfo struct {
	PageW         int32
	PageH         int32
	AverageImageW int32
	AverageImageH int32
	ImageCount    int32
	PageCount     int32
	Size          int64
	File          string
	DstDir        string
	TextLen       int32
	InfoDone      bool
	ConvDone      bool
	ImgsDone      bool
	ToTextDone    bool
	LockDone      bool
	InfoErr       bool
	ConvErr       bool
	ImgsErr       bool
	ToTextErr     bool
	LockErr       bool
}

func main() {

	if runtime.GOOS == "windows" {
		fmt.Printf("windows\n")
	} else if runtime.GOOS == "linux" {
		fmt.Printf("linux\n")
	} else if runtime.GOOS == "darwin" {
		fmt.Printf("unsupported darwin\n")
	}

	if len(os.Args) < 5 {
		fmt.Printf(`Missing argument
Expectng tree-converter srcDir dstDir password delout|nodelout
`)
		return
	}

	start := time.Now()

	dstDir := os.Args[2]

	_, err := os.Stat(dstDir)
	if err != nil {
		os.MkdirAll(dstDir, 0777)
	}

	context := Computation{
		files: make(chan string),
		///pdfInfo:        make(chan PdfInfo),
		filePattern:    `.*\.pdf$`,
		PageCount:      0,
		TotalInputSize: 0,
		srcDir:         os.Args[1],
		dstDir:         os.Args[2],
		userPasswd:     os.Args[3],
		deleteOutput:   os.Args[4] == "delout",
		fileCount:      0,
		matchedCount:   0,
		finishedCount:  0,
		skipPdfInfo:    false,
		skipPdfConvert: true,
		skipListImages: true,
		skipToText:     true,
		skipLockFile:   false,
		fileInfos:      make(map[string]*FileInfo),
		fileInfosLock:  sync.RWMutex{},
	}

	ticker := time.NewTicker(time.Millisecond * 500)
	go startNotifier(&context, ticker)

	context.tasks.Add(1)
	go collectFiles(&context)

	context.tasks.Add(1)
	go processFiles(&context, true)

	context.tasks.Wait()

	ticker.Stop()

	end := time.Now()

	fmt.Printf("\nTotal pages: %d took: %f seconds\n", context.PageCount, float64(end.Sub(start))/1000.0/1000.0/1000.0)

	doSynced(&context.fileInfosLock, func() {
		infos := make([]*FileInfo, len(context.fileInfos))
		i := 0
		csvFile, err := os.Create(filepath.Join(dstDir, "file-info.csv"))
		check(err)
		defer csvFile.Close()
		csvWr := csv.NewWriter(csvFile)
		csvWr.Comma = '\t'
		csvWr.Write([]string{
			"File",
			"DstDir",
			"ImageCount",
			"PageCount",
			"AverageImageW",
			"AverageImageH",
			"PageW",
			"PageH",
			"Size",
			"TextLen",
			"ScanPdf",
			"InfoDone",
			"ImgsDone",
			"ConvDone",
			"ToTextDone",
			"LockDone",
		})

		for _, val := range context.fileInfos {
			infos[i] = val
			scanPdf := "false"
			if val.PageCount == val.ImageCount && val.TextLen < 500 {
				scanPdf = "true"
			}
			csvWr.Write([]string{
				val.File,
				val.DstDir,
				strconv.FormatInt(int64(val.ImageCount), 10),
				strconv.FormatInt(int64(val.PageCount), 10),
				strconv.FormatInt(int64(val.AverageImageW), 10),
				strconv.FormatInt(int64(val.AverageImageH), 10),
				strconv.FormatInt(int64(val.PageW), 10),
				strconv.FormatInt(int64(val.PageH), 10),
				strconv.FormatInt(int64(val.Size), 10),
				strconv.FormatInt(int64(val.TextLen), 10),
				scanPdf,
				strconv.FormatBool(val.InfoErr),
				strconv.FormatBool(val.ImgsErr),
				strconv.FormatBool(val.ConvErr),
				strconv.FormatBool(val.ToTextErr),
				strconv.FormatBool(val.LockErr),
			})
			i++
		}
		json, _ := json.MarshalIndent(infos, "", "  ")
		ioutil.WriteFile(filepath.Join(dstDir, "file-info.json"), json, 0777)
	})

}

func startNotifier(context *Computation, ticker *time.Ticker) {
	lastFinished := uint64(0)
	lastTime := time.Now()
	for range ticker.C {
		doSynced(&context.fileInfosLock, func() {
			current := time.Now()

			diffSec := current.Sub(lastTime).Seconds()
			curFinished := context.finishedCount
			diffFinished := (curFinished - lastFinished)
			perSec := float64(diffFinished) / diffSec

			lastFinished = curFinished
			lastTime = current

			fmt.Printf("scanned: %5d matched %5d finished  %5d (%4f/sec) page count: %6d image count: %7d total input size: %7d\r",
				context.fileCount, context.matchedCount, context.finishedCount, perSec, context.PageCount, context.imageCount, context.TotalInputSize)

		})
	}
}

type SynchronizedTask func()

func doSynced(lock *sync.RWMutex, task SynchronizedTask) {
	lock.Lock()
	task()
	lock.Unlock()
}

func check(err error) {
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		panic(err.Error())
	}
}

/*
======================================================================================
*/
func processFiles(context *Computation, goroutine bool) {
	if goroutine {
		defer context.tasks.Done()
	}

	for i := 0; i < workerCount(); i++ {
		context.tasks.Add(1)
		go fileWorker(context, true)
	}

}

/*
======================================================================================
*/
func fileWorker(context *Computation, goroutine bool) {
	if goroutine {
		defer context.tasks.Done()
	}
	for file := range context.files {
		//context.tasks.Add(1)
		processFile(file, context, false)
	}
}

func mkOutDir(file string, context *Computation, mkdir bool) string {
	rel, _ := filepath.Rel(context.srcDir, file)
	dstFileDir := filepath.Join(context.dstDir, rel+"-out")
	if mkdir {
		if err := os.MkdirAll(dstFileDir, 0774); err != nil {
			fmt.Printf("failed to create file dst dir: %s erroe: %s\n", dstFileDir, err.Error())
			return ""
		}
	}
	return dstFileDir
}

/*
======================================================================================
*/
func processFile(file string, context *Computation, goroutine bool) {

	if goroutine {
		defer context.tasks.Done()
	}

	fi := FileInfo{
		File:       file,
		DstDir:     mkOutDir(file, context, true),
		ImgsDone:   context.skipListImages,
		InfoDone:   context.skipPdfInfo,
		ConvDone:   context.skipPdfConvert,
		LockDone:   context.skipLockFile,
		ToTextDone: context.skipToText,
		InfoErr:    false,
		ConvErr:    false,
		ImgsErr:    false,
		ToTextErr:  false,
		LockErr:    false,
	}

	doSynced(&context.fileInfosLock, func() {
		context.fileInfos[file] = &fi
	})

	if !context.skipPdfInfo {
		//context.tasks.Add(1)
		createPdfInfo(file, fi.DstDir, context, false)
	}

	if !context.skipPdfConvert {
		//context.tasks.Add(1)
		convertFile(file, fi.DstDir, context, false)
	}

	if !context.skipListImages {
		//context.tasks.Add(1)
		listImages(file, fi.DstDir, context, false)
	}

	if !context.skipToText {
		//context.tasks.Add(1)
		toText(file, fi.DstDir, context, false)
	}

	if !context.skipLockFile {
		lockFile(file, fi.DstDir, context, false)
	}
}

func match2map(re *regexp.Regexp, s string) map[string]string {
	match := re.FindAllStringSubmatch(s, -1)
	fields := make(map[string]string)
	for _, m := range match {
		//			fmt.Printf("%d one match %s\n", mIdx, m)
		for nIdx, name := range re.SubexpNames()[1:] {
			if m[nIdx+1] != "" {
				fields[name] = m[nIdx+1]
			}
		}
	}
	///fmt.Printf("fields: %s\n", fields)
	return fields
}

func ensureInfoFile(context *Computation) (string, error) {
	infoFile := filepath.Join(context.dstDir, "info.txt")

	_, statErr := os.Stat(infoFile)
	if statErr == nil {
		return infoFile, nil
	}

	infoFileContents := fmt.Sprintf(`InfoBegin
InfoKey: Keywords
InfoValue: ID-%s
`, filepath.Base(context.dstDir))

	writeErr := ioutil.WriteFile(infoFile, []byte(infoFileContents), 0644)
	if writeErr == nil {
		return infoFile, nil
	} else {
		return "", writeErr
	}
}

/*
======================================================================================
*/
func lockFile(file string, dstFileDir string, context *Computation, goroutine bool) {
	err := false
	if goroutine {
		defer context.tasks.Done()
	}

	/*
		tmpFile, tmpErr := ioutil.TempFile("", "xxxx")
		if tmpErr != nil {
			fmt.Printf("Error creating temp file %s\n", tmpErr.Error())
			return
		}
		tmpFileName, _ := filepath.Abs(tmpFile.Name()) //FIXME not ignore error
		tmpFile.Close()
		tmpFileNamePdf := tmpFileName + ".pdf" //FIXME not ignore error

		defer os.Remove(tmpFileNamePdf)
		defer os.Remove(tmpFileName)

		cmdAddKw := "exiftool"
		cmdAddKwArgs := []string{fmt.Sprintf("-Keywords+=ID-%s", filepath.Base(context.dstDir)),
			"-o", tmpFileNamePdf,
			file}

		addKeywordsCmd := exec.Command(cmdAddKw, cmdAddKwArgs...)

		outKw, outKwErr := addKeywordsCmd.CombinedOutput()
		if outKwErr != nil {
			fmt.Printf("Error while adding kw %s\nOut:%s\nCmd:exiftool %s\n", outKwErr.Error(), outKw, cmdAddKwArgs)
			return
		}
	*/

	infoFile, infoFileErr := ensureInfoFile(context)
	if infoFileErr != nil {
		fmt.Printf("failed to create info.txt in %s: %s\n", context.dstDir, infoFileErr.Error())
		return
	}

	//fmt.Printf("locking %s <<\n", dstFileDir)

	if runtime.GOOS == "linux" && false {
		dstDir, dstErr := filepath.Abs(context.dstDir)
		infoFileFile := filepath.Base(infoFile)
		inDir, inErr := filepath.Abs(filepath.Dir(file))
		inFile := filepath.Base(file)
		outDir, outErr := filepath.Abs(filepath.Dir(dstFileDir))
		outFile := filepath.Base(file)
		outFileAbs, _ := filepath.Abs(filepath.Join(outDir, outFile))
		if context.deleteOutput {
			defer os.Remove(outFileAbs)
		}

		if inErr != nil {
			fmt.Printf("failed to abs of in dir %s", inErr.Error())
			return
		}

		if outErr != nil {
			fmt.Printf("failed to abs of out dir %s", outErr.Error())
			return
		}

		if dstErr != nil {
			fmt.Printf("failed to abs of out dir %s", dstErr.Error())
			return
		}

		//
		cmd := "docker"
		cmdArgs := []string{"run", "--rm",
			"-v", inDir + ":/work", "-v", outDir + ":/out", "-v", dstDir + ":/dst",
			"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
			"mnuessler/pdftk",
			"/work/" + inFile, "update_info", "/dst/" + infoFileFile, "output", "/out/" + outFile,
			"encrypt_128bit", "owner_pw", "owner", "user_pw", "user"}

		lockCmd := exec.Command(cmd, cmdArgs...)

		///convertCmd := exec.Command("pdftopng", "-r", "300", file, filepath.Join(dstFileDir, "pages"))
		out, err := lockCmd.CombinedOutput()
		if err != nil {
			fmt.Printf("Failed to lock file %s: %s\nCommand:\n%s\n", file, err.Error(), cmd, cmdArgs)
			err = true
		}
		ioutil.WriteFile(filepath.Join(dstFileDir, "lock.txt"), out, 0777)
	}

	if runtime.GOOS == "windows" || true {
		base := filepath.Base(file)
		dir := filepath.Dir(dstFileDir)
		outFile := filepath.Join(dir, base)
		outFileAbs, _ := filepath.Abs(filepath.Join(dir, base))
		if context.deleteOutput {
			defer os.Remove(outFileAbs)
		}

		//
		cmd := "pdftk"
		cmdArgs := []string{file, "update_info", infoFile, "output", outFile,
			"encrypt_128bit", "owner_pw", "owner", "user_pw", "user"}

		lockCmd := exec.Command(cmd, cmdArgs...)

		///convertCmd := exec.Command("pdftopng", "-r", "300", file, filepath.Join(dstFileDir, "pages"))
		out, err := lockCmd.Output()
		if err != nil {
			fmt.Printf("Failed to lock file %s: %s\nCommand:\n%s\n", file, err.Error(), cmd, cmdArgs)
			err = true
		}
		ioutil.WriteFile(filepath.Join(dstFileDir, "lock.txt"), out, 0777)
	}

	doSynced(&context.fileInfosLock, func() {
		fileInfo := context.fileInfos[file]
		fileInfo.LockDone = true
		if err {
			fileInfo.LockErr = true
		}

		if fileFinished(fileInfo) {
			context.finishedCount++
		}
	})
}

/*
======================================================================================
*/
func toText(file string, dstFileDir string, context *Computation, goroutine bool) {
	if goroutine {
		defer context.tasks.Done()
	}

	toTextCmd := exec.Command("pdftotext", file, "-")
	textBytes, err := toTextCmd.Output()

	if err != nil {
		fmt.Printf("Failed to get text from file %s: %s\n", file, err.Error())
		context.fileInfos[file].ToTextErr = true
		return
	}

	text := string(textBytes)
	ioutil.WriteFile(filepath.Join(dstFileDir, "text.txt"), textBytes, 0777)

	doSynced(&context.fileInfosLock, func() {
		context.fileInfos[file].TextLen = int32(len(text))
		context.fileInfos[file].ToTextDone = true
		if fileFinished(context.fileInfos[file]) {
			context.finishedCount++
		}
	})
}

/*
======================================================================================
*/
func createPdfInfo(file string, dstFileDir string, context *Computation, goroutine bool) {
	if goroutine {
		defer context.tasks.Done()
	}

	convertCmd := exec.Command("pdfinfo", file)
	pdfinfo, err := convertCmd.Output()

	if err != nil {
		fmt.Printf("Failed to get info on file %s: %s\n", file, err.Error())
		context.fileInfos[file].InfoErr = true
	} else {
		ioutil.WriteFile(filepath.Join(dstFileDir, "pdfinfo.txt"), pdfinfo, 0777)

		pdfinfoStr := string(pdfinfo)
		pattern := regexp.MustCompile("Pages: +(?P<pages>[0-9]+)|File size: +(?P<size>[0-9]+) |PDF version: +(?P<version>[0-9.]+)|Page size: +(?P<width>[0-9.]+) x (?P<height>[0-9.]+) pts")

		fields := match2map(pattern, pdfinfoStr)
		//fmt.Printf("page info: %s\n", fields)

		size, errSize := strconv.ParseInt(fields["size"], 10, 32)
		pages, errPages := strconv.ParseInt(fields["pages"], 10, 32)
		pageW, errPW := strconv.ParseFloat(fields["width"], 64)
		pageH, errPH := strconv.ParseFloat(fields["height"], 64)

		if errSize != nil {
			size = 0
		}
		if errPages != nil {
			pages = 0
		}
		if errPW != nil {
			pageW = 0
		}
		if errPH != nil {
			pageH = 0
		}

		atomic.AddUint64(&context.PageCount, uint64(pages))
		atomic.AddUint64(&context.TotalInputSize, uint64(size))

		var fileInfo *FileInfo
		doSynced(&context.fileInfosLock, func() {
			fileInfo = context.fileInfos[file]
			fileInfo.PageW = int32(pageW)
			fileInfo.PageH = int32(pageH)
			fileInfo.PageCount = int32(pages)
			fileInfo.Size = size
			fileInfo.InfoDone = true
			if fileFinished(fileInfo) {
				context.finishedCount++
			}
		})

		/*
			return PdfInfo{
				filename: file,
				size:     int(size),
				pages:    int(pages),
			}
		*/
	}
}

/*
======================================================================================
*/
func convertFile(file string, dstFileDir string, context *Computation, goroutine bool) {
	err := false
	if goroutine {
		defer context.tasks.Done()
	}

	convertCmd := exec.Command("pdftopng", "-r", "300", file, filepath.Join(dstFileDir, "pages"))
	out, err := convertCmd.Output()
	if err != nil {
		fmt.Printf("Failed to get info on file %s: %s\n", file, err.Error())
		err = true
	} else {
		ioutil.WriteFile(filepath.Join(dstFileDir, "convert.txt"), out, 0777)
	}

	doSynced(&context.fileInfosLock, func() {
		fileInfo := context.fileInfos[file]
		fileInfo.ConvDone = true
		if err {
			context.fileInfos[file].ImgsErr = true
		}
		if fileFinished(fileInfo) {
			context.finishedCount++
		}
	})
}

/*
======================================================================================
*/
func listImages(file string, dstFileDir string, context *Computation, goroutine bool) {
	if goroutine {
		defer context.tasks.Done()
	}

	commandArray := []string{"pdfimages", "-list", file}
	listImagesCmd := exec.Command("pdfimages", "-list", file)
	out, err := listImagesCmd.Output()
	if err != nil {
		fmt.Printf("Failed to get image list on file %s: %s\n", file, err.Error())
		fmt.Printf("Command: %s\n", strings.Join(commandArray, " "))
	} else {
		ioutil.WriteFile(filepath.Join(dstFileDir, "image.list"), out, 0777)
	}
	//page   num  type   width height color comp bpc  enc interp  object ID x-ppi y-ppi size ratio
	linePat := regexp.MustCompile(`(?P<page>[0-9]+) +(?P<num>[0-9]+) +(?P<type>[a-z0-9]+) +(?P<width>[0-9]+) +(?P<height>[0-9]+) +(?P<color>[a-z0-9]+) +(?P<comp>[0-9]+) +(?P<bpc>[0-9]+) +(?P<enc>[a-z0-9]+) +(?P<interp>[a-z0-9]+) +(?P<object>[0-9]+) +(?P<ID>[0-9]+) +(?P<xppi>[0-9]+) +(?P<yppi>[0-9]+) +(?P<size>[^ ]+) +(?P<ratio>[^ ]+)`)
	outStr := string(out)

	imageCount := 0
	imageWSum := int64(0)
	imageHSum := int64(0)
	for _, ln := range strings.Split(outStr, "\n") {
		fields := match2map(linePat, ln)
		if len(fields["page"]) > 0 {
			//have image info
			imageW, errW := strconv.ParseInt(fields["width"], 10, 32)
			imageH, errH := strconv.ParseInt(fields["height"], 10, 32)
			imageXR, errXR := strconv.ParseInt(fields["xppi"], 10, 32)
			imageYR, errYR := strconv.ParseInt(fields["yppi"], 10, 32)
			if errW == nil && errH == nil && errXR == nil && errYR == nil {
				ptW := 72 * imageW / imageXR
				ptH := 72 * imageH / imageYR

				imageCount++
				imageWSum += ptW
				imageHSum += ptH
			}
		}
	}

	doSynced(&context.fileInfosLock, func() {
		fi := context.fileInfos[file]
		fi.ImgsDone = true
		if imageCount == 0 {
			fi.AverageImageH = 0
			fi.AverageImageW = 0
		} else {
			fi.AverageImageH = int32(imageHSum / int64(imageCount))
			fi.AverageImageW = int32(imageWSum / int64(imageCount))
			fi.ImageCount = int32(imageCount)
		}
		context.imageCount += uint64(imageCount)
		if fileFinished(fi) {
			context.finishedCount += 1
		}
	})
}

/*
======================================================================================
*/
func fileFinished(fi *FileInfo) bool {
	//fmt.Printf("%s %s %s %s\n", fi.ConvDone, fi.ImgsDone, fi.InfoDone, fi.ToTextDone)
	return fi.ConvDone && fi.ImgsDone && fi.InfoDone && fi.ToTextDone && fi.LockDone
}

/*
======================================================================================
*/
func collectFiles(context *Computation) {
	defer context.tasks.Done()

	filePattern := regexp.MustCompile(context.filePattern)
	filepath.Walk(context.srcDir, func(path string, info os.FileInfo, err error) error {
		context.fileCount++
		if filePattern.MatchString(strings.ToLower(path)) {
			context.matchedCount++
			context.files <- path
		}
		return nil
	})
	close(context.files)
}
