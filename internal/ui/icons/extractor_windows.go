//go:build windows

package icons

import (
	"errors"
	"image"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	modOle32   = windows.NewLazySystemDLL("ole32.dll")
	modShell32 = windows.NewLazySystemDLL("shell32.dll")
	modUser32  = windows.NewLazySystemDLL("user32.dll")
	modGdi32   = windows.NewLazySystemDLL("gdi32.dll")

	procCoInitializeEx     = modOle32.NewProc("CoInitializeEx")
	procCoUninitialize     = modOle32.NewProc("CoUninitialize")
	procSHGetFileInfoW     = modShell32.NewProc("SHGetFileInfoW")
	procGetIconInfo        = modUser32.NewProc("GetIconInfo")
	procDestroyIcon        = modUser32.NewProc("DestroyIcon")
	procCreateCompatibleDC = modGdi32.NewProc("CreateCompatibleDC")
	procDeleteDC           = modGdi32.NewProc("DeleteDC")
	procDeleteObject       = modGdi32.NewProc("DeleteObject")
	procGetObjectW         = modGdi32.NewProc("GetObjectW")
	procGetDIBits          = modGdi32.NewProc("GetDIBits")
)

const (
	coinitApartmentThreaded = 0x2
	coinitDisableOLE1DDE    = 0x4
	shgfiIcon               = 0x000000100
	shgfiSmallIcon          = 0x000000001
	shgfiUseFileAttributes  = 0x000000010
	fileAttrNormal          = 0x00000080
	fileAttrDirectory       = 0x00000010
	dibRGBColors            = 0
	biRGB                   = 0
)

type shFileInfoW struct {
	HIcon         uintptr
	IIcon         int32
	DwAttributes  uint32
	SzDisplayName [260]uint16
	SzTypeName    [80]uint16
}

type iconInfo struct {
	FIcon    int32
	XHotspot uint32
	YHotspot uint32
	HbmMask  uintptr
	HbmColor uintptr
}

type gdiBitmap struct {
	BmType       int32
	BmWidth      int32
	BmHeight     int32
	BmWidthBytes int32
	BmPlanes     uint16
	BmBitsPixel  uint16
	BmBits       uintptr
}

type bitmapInfoHeader struct {
	BiSize          uint32
	BiWidth         int32
	BiHeight        int32
	BiPlanes        uint16
	BiBitCount      uint16
	BiCompression   uint32
	BiSizeImage     uint32
	BiXPelsPerMeter int32
	BiYPelsPerMeter int32
	BiClrUsed       uint32
	BiClrImportant  uint32
}

type windowsExtractor struct{}

func newPlatformExtractor() Extractor {
	return &windowsExtractor{}
}

func (w *windowsExtractor) ExtractExt(ext string, isDir bool) (image.Image, error) {
	if isDir {
		return extractWinIcon("folder", fileAttrDirectory, shgfiIcon|shgfiSmallIcon|shgfiUseFileAttributes)
	}
	name := "file"
	if ext != "" {
		if ext[0] != '.' {
			name += "." + ext
		} else {
			name += ext
		}
	}
	return extractWinIcon(name, fileAttrNormal, shgfiIcon|shgfiSmallIcon|shgfiUseFileAttributes)
}

func (w *windowsExtractor) ExtractFile(absPath string) (image.Image, error) {
	if absPath == "" {
		return nil, errors.New("icons: empty path")
	}
	return extractWinIcon(absPath, 0, shgfiIcon|shgfiSmallIcon)
}

func extractWinIcon(path string, attrs uint32, flags uint32) (image.Image, error) {
	u16, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	// MSDN requires COM initialization on the calling OS thread before SHGetFileInfoW.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	hr, _, _ := procCoInitializeEx.Call(0, uintptr(coinitApartmentThreaded|coinitDisableOLE1DDE))
	if hr == 0 || hr == 1 { // S_OK or S_FALSE
		defer procCoUninitialize.Call()
	}

	var shfi shFileInfoW
	r1, _, _ := procSHGetFileInfoW.Call(
		uintptr(unsafe.Pointer(u16)),
		uintptr(attrs),
		uintptr(unsafe.Pointer(&shfi)),
		unsafe.Sizeof(shfi),
		uintptr(flags),
	)
	if r1 == 0 || shfi.HIcon == 0 {
		return nil, errors.New("icons: SHGetFileInfoW returned no icon")
	}
	defer procDestroyIcon.Call(shfi.HIcon)

	return hiconToRGBA(shfi.HIcon)
}

func hiconToRGBA(hIcon uintptr) (*image.RGBA, error) {
	var ii iconInfo
	r1, _, _ := procGetIconInfo.Call(hIcon, uintptr(unsafe.Pointer(&ii)))
	if r1 == 0 {
		return nil, errors.New("icons: GetIconInfo failed")
	}
	if ii.HbmMask != 0 {
		defer procDeleteObject.Call(ii.HbmMask)
	}
	if ii.HbmColor != 0 {
		defer procDeleteObject.Call(ii.HbmColor)
	}
	if ii.HbmColor == 0 {
		return nil, errors.New("icons: icon has no color bitmap")
	}

	var bm gdiBitmap
	rObj, _, _ := procGetObjectW.Call(ii.HbmColor, unsafe.Sizeof(bm), uintptr(unsafe.Pointer(&bm)))
	if rObj == 0 || bm.BmWidth <= 0 || bm.BmHeight <= 0 || bm.BmWidth > 256 || bm.BmHeight > 256 {
		return nil, errors.New("icons: invalid icon bitmap dimensions")
	}

	hdc, _, _ := procCreateCompatibleDC.Call(0)
	if hdc == 0 {
		return nil, errors.New("icons: CreateCompatibleDC failed")
	}
	defer procDeleteDC.Call(hdc)

	w := int(bm.BmWidth)
	h := int(bm.BmHeight)
	var bih bitmapInfoHeader
	bih.BiSize = uint32(unsafe.Sizeof(bih))
	bih.BiWidth = int32(w)
	bih.BiHeight = -int32(h) // top-down DIB
	bih.BiPlanes = 1
	bih.BiBitCount = 32
	bih.BiCompression = biRGB

	buf := make([]byte, w*h*4)
	lines, _, _ := procGetDIBits.Call(
		hdc,
		ii.HbmColor,
		0,
		uintptr(h),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(unsafe.Pointer(&bih)),
		dibRGBColors,
	)
	if lines == 0 {
		return nil, errors.New("icons: GetDIBits failed")
	}

	// Convert BGRA -> RGBA and check if alpha channel is all zero (legacy icons).
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	hasAlpha := false
	for i := 0; i < len(buf); i += 4 {
		b, g, r, a := buf[i], buf[i+1], buf[i+2], buf[i+3]
		if a != 0 {
			hasAlpha = true
		}
		img.Pix[i] = r
		img.Pix[i+1] = g
		img.Pix[i+2] = b
		img.Pix[i+3] = a
	}
	if !hasAlpha {
		for i := 3; i < len(img.Pix); i += 4 {
			img.Pix[i] = 255
		}
	}
	return img, nil
}
