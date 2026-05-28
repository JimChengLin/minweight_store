//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/JimChengLin/minpatricia"
)

const (
	mmapNodeExtentBytes           = 16 * 1024 * 1024
	mmapNodePageSize              = minpatricia.NodeSize
	mmapNodePagesPerExtent        = mmapNodeExtentBytes / mmapNodePageSize
	mmapNodeReservedPages         = 2
	mmapNodeSlotsPerExtent        = mmapNodePagesPerExtent - mmapNodeReservedPages
	mmapNodeBitmapBytes           = (mmapNodeSlotsPerExtent + 7) / 8
	mmapNodeMetaVersion    uint32 = 1
)

var mmapNodeMetaMagic = [8]byte{'M', 'W', 'N', 'O', 'D', 'E', '0', '1'}

// Extent layout: page 0 meta, page 1 allocation bitmap, pages 2..4095 nodes.
type mmapNodeStore struct {
	dir     string
	extents []*mmapNodeExtent
	pages   []*minpatricia.NodePage
}

type mmapNodeExtent struct {
	id   uint64
	path string
	file *os.File
	data []byte
}

func openMmapNodeStore(dir string) (*mmapNodeStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	extents, err := openMmapNodeExtents(dir)
	if err != nil {
		return nil, err
	}
	store := &mmapNodeStore{
		dir:     dir,
		extents: extents,
	}
	if len(store.extents) == 0 {
		extent, err := createMmapNodeExtent(dir, 0)
		if err != nil {
			return nil, err
		}
		extent.setUsed(0, true)
		extent.setLiveSlots(1)
		store.extents = append(store.extents, extent)
	}
	store.rebuildPageIndex()
	if store.extents[0] == nil || store.pages[0] == nil {
		_ = store.Close()
		return nil, fmt.Errorf("minweight_store: mmap node root is not allocated")
	}
	return store, nil
}

func openMmapNodeExtents(dir string) ([]*mmapNodeExtent, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ids []uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".nodes") {
			continue
		}
		id, err := parseMmapNodeExtentID(entry.Name())
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	if len(ids) == 0 {
		return nil, nil
	}

	extents := make([]*mmapNodeExtent, int(ids[len(ids)-1])+1)
	for _, id := range ids {
		extent, err := openMmapNodeExtent(filepath.Join(dir, mmapNodeExtentName(id)), id)
		if err != nil {
			closeMmapNodeExtents(extents)
			return nil, err
		}
		extents[id] = extent
	}
	return extents, nil
}

func (s *mmapNodeStore) Root() uint64 {
	return 0
}

func (s *mmapNodeStore) Get(id uint64) (*minpatricia.NodePage, error) {
	if id >= uint64(len(s.pages)) || s.pages[id] == nil {
		return nil, minpatricia.ErrCorruptLayout
	}
	return s.pages[id], nil
}

func (s *mmapNodeStore) Alloc() (uint64, *minpatricia.NodePage, error) {
	for extentID, extent := range s.extents {
		if extent == nil {
			id := uint64(extentID) * mmapNodeSlotsPerExtent
			if id&minpatriciaHandleTag != 0 {
				return 0, nil, minpatricia.ErrPositionTag
			}
			extent, err := createMmapNodeExtent(s.dir, uint64(extentID))
			if err != nil {
				return 0, nil, err
			}
			s.extents[extentID] = extent
			extent.setUsed(0, true)
			extent.setLiveSlots(1)
			page := extent.page(0)
			s.setPage(id, page)
			return id, page, nil
		}
		if extent.liveSlots() == mmapNodeSlotsPerExtent {
			continue
		}
		id, page, ok := extent.alloc()
		if !ok {
			continue
		}
		if id&minpatriciaHandleTag != 0 {
			return 0, nil, minpatricia.ErrPositionTag
		}
		s.setPage(id, page)
		return id, page, nil
	}

	extentID := uint64(len(s.extents))
	id := extentID * mmapNodeSlotsPerExtent
	if id&minpatriciaHandleTag != 0 {
		return 0, nil, minpatricia.ErrPositionTag
	}
	extent, err := createMmapNodeExtent(s.dir, extentID)
	if err != nil {
		return 0, nil, err
	}
	s.extents = append(s.extents, extent)
	extent.setUsed(0, true)
	extent.setLiveSlots(1)
	page := extent.page(0)
	s.setPage(id, page)
	return id, page, nil
}

func (s *mmapNodeStore) Free(id uint64) error {
	if id == s.Root() {
		return minpatricia.ErrCorruptLayout
	}
	extent, slot, err := s.extentFor(id)
	if err != nil {
		return err
	}
	if !extent.used(slot) {
		return minpatricia.ErrCorruptLayout
	}
	extent.setUsed(slot, false)
	extent.setLiveSlots(extent.liveSlots() - 1)
	s.setPage(id, nil)
	if extent.liveSlots() == 0 {
		return s.releaseFreeExtents()
	}
	return nil
}

func (s *mmapNodeStore) LiveNodes() int {
	total := 0
	for _, extent := range s.extents {
		if extent == nil {
			continue
		}
		total += int(extent.liveSlots())
	}
	return total
}

func (s *mmapNodeStore) Sync() error {
	for _, extent := range s.extents {
		if extent == nil {
			continue
		}
		if err := extent.sync(); err != nil {
			return err
		}
	}
	return nil
}

func (s *mmapNodeStore) Close() error {
	var firstErr error
	for _, extent := range s.extents {
		if extent == nil {
			continue
		}
		if err := extent.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.extents = nil
	return firstErr
}

func (s *mmapNodeStore) closeAfterSync() error {
	var firstErr error
	for _, extent := range s.extents {
		if extent == nil {
			continue
		}
		if err := extent.closeAfterSync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.extents = nil
	return firstErr
}

func (s *mmapNodeStore) Reset() error {
	if len(s.extents) == 0 || s.extents[0] == nil {
		return minpatricia.ErrCorruptLayout
	}
	var firstErr error
	for id := len(s.extents) - 1; id >= 1; id-- {
		extent := s.extents[id]
		if extent == nil {
			continue
		}
		if err := extent.destroy(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.extents[id] = nil
	}
	if firstErr != nil {
		return firstErr
	}

	s.extents = s.extents[:1]
	root := s.extents[0]
	clear(root.bitmap())
	root.setUsed(0, true)
	root.setLiveSlots(1)
	clear(mmapNodePageBytes(root.page(0)))
	s.rebuildPageIndex()
	return nil
}

func (s *mmapNodeStore) extentFor(id uint64) (*mmapNodeExtent, uint64, error) {
	if id&minpatriciaHandleTag != 0 {
		return nil, 0, minpatricia.ErrPositionTag
	}
	extentID := id / mmapNodeSlotsPerExtent
	slot := id % mmapNodeSlotsPerExtent
	if extentID >= uint64(len(s.extents)) {
		return nil, 0, minpatricia.ErrCorruptLayout
	}
	extent := s.extents[extentID]
	if extent == nil || extent.id != extentID {
		return nil, 0, minpatricia.ErrCorruptLayout
	}
	return extent, slot, nil
}

func (s *mmapNodeStore) rebuildPageIndex() {
	s.pages = nil
	for _, extent := range s.extents {
		if extent == nil {
			continue
		}
		s.ensurePageSlots(extent.id)
		for slot := uint64(0); slot < mmapNodeSlotsPerExtent; slot++ {
			if extent.used(slot) {
				s.pages[extent.id*mmapNodeSlotsPerExtent+slot] = extent.page(slot)
			}
		}
	}
}

func (s *mmapNodeStore) setPage(id uint64, page *minpatricia.NodePage) {
	extentID := id / mmapNodeSlotsPerExtent
	s.ensurePageSlots(extentID)
	s.pages[id] = page
}

func (s *mmapNodeStore) ensurePageSlots(extentID uint64) {
	need := int((extentID + 1) * mmapNodeSlotsPerExtent)
	if len(s.pages) >= need {
		return
	}
	s.pages = append(s.pages, make([]*minpatricia.NodePage, need-len(s.pages))...)
}

func (s *mmapNodeStore) releaseFreeExtents() error {
	keep := -1
	freeCount := 0
	for id, extent := range s.extents {
		if extent == nil || extent.liveSlots() != 0 {
			continue
		}
		freeCount++
		if keep == -1 {
			keep = id
		}
	}
	if freeCount <= 1 {
		return nil
	}

	for id := len(s.extents) - 1; id >= 0; id-- {
		extent := s.extents[id]
		if extent == nil || extent.liveSlots() != 0 || id == keep {
			continue
		}
		if err := extent.destroy(); err != nil {
			return err
		}
		s.extents[id] = nil
		freeCount--
		if freeCount == 1 {
			break
		}
	}
	s.trimReleasedExtents()
	return nil
}

func (s *mmapNodeStore) trimReleasedExtents() {
	for len(s.extents) != 0 && s.extents[len(s.extents)-1] == nil {
		s.extents = s.extents[:len(s.extents)-1]
	}
	maxPages := len(s.extents) * mmapNodeSlotsPerExtent
	if len(s.pages) > maxPages {
		s.pages = s.pages[:maxPages]
	}
}

func createMmapNodeExtent(dir string, id uint64) (*mmapNodeExtent, error) {
	path := filepath.Join(dir, mmapNodeExtentName(id))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}

	extentOwnedByCaller := false
	defer func() {
		if !extentOwnedByCaller {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()

	if err := file.Truncate(mmapNodeExtentBytes); err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, mmapNodeExtentBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	extent := &mmapNodeExtent{
		id:   id,
		path: path,
		file: file,
		data: data,
	}
	extent.writeMeta(0)
	extentOwnedByCaller = true
	return extent, nil
}

func openMmapNodeExtent(path string, id uint64) (*mmapNodeExtent, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	extentOwnedByCaller := false
	defer func() {
		if !extentOwnedByCaller {
			_ = file.Close()
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != mmapNodeExtentBytes {
		return nil, fmt.Errorf("minweight_store: mmap node extent %s size = %d, want %d", path, info.Size(), mmapNodeExtentBytes)
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, mmapNodeExtentBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	extent := &mmapNodeExtent{
		id:   id,
		path: path,
		file: file,
		data: data,
	}
	if err := extent.validateMeta(); err != nil {
		_ = syscall.Munmap(data)
		return nil, err
	}
	extentOwnedByCaller = true
	return extent, nil
}

func closeMmapNodeExtents(extents []*mmapNodeExtent) {
	for _, extent := range extents {
		if extent == nil {
			continue
		}
		_ = extent.close()
	}
}

func mmapNodeExtentName(id uint64) string {
	return fmt.Sprintf("%020d.nodes", id)
}

func parseMmapNodeExtentID(name string) (uint64, error) {
	if len(name) != len("00000000000000000000.nodes") || !strings.HasSuffix(name, ".nodes") {
		return 0, fmt.Errorf("minweight_store: invalid mmap node extent name %q", name)
	}
	return strconv.ParseUint(strings.TrimSuffix(name, ".nodes"), 10, 64)
}

func (e *mmapNodeExtent) writeMeta(liveSlots uint32) {
	copy(e.data[0:8], mmapNodeMetaMagic[:])
	binary.LittleEndian.PutUint32(e.data[8:12], mmapNodeMetaVersion)
	binary.LittleEndian.PutUint32(e.data[12:16], mmapNodePageSize)
	binary.LittleEndian.PutUint64(e.data[16:24], e.id)
	binary.LittleEndian.PutUint32(e.data[24:28], mmapNodeSlotsPerExtent)
	binary.LittleEndian.PutUint32(e.data[28:32], liveSlots)
}

func (e *mmapNodeExtent) validateMeta() error {
	if !bytes.Equal(e.data[0:8], mmapNodeMetaMagic[:]) {
		return fmt.Errorf("minweight_store: mmap node extent %s has invalid magic", e.path)
	}
	if version := binary.LittleEndian.Uint32(e.data[8:12]); version != mmapNodeMetaVersion {
		return fmt.Errorf("minweight_store: mmap node extent %s version = %d, want %d", e.path, version, mmapNodeMetaVersion)
	}
	if pageSize := binary.LittleEndian.Uint32(e.data[12:16]); pageSize != mmapNodePageSize {
		return fmt.Errorf("minweight_store: mmap node extent %s page size = %d, want %d", e.path, pageSize, mmapNodePageSize)
	}
	if id := binary.LittleEndian.Uint64(e.data[16:24]); id != e.id {
		return fmt.Errorf("minweight_store: mmap node extent %s id = %d, want %d", e.path, id, e.id)
	}
	if slots := binary.LittleEndian.Uint32(e.data[24:28]); slots != mmapNodeSlotsPerExtent {
		return fmt.Errorf("minweight_store: mmap node extent %s slots = %d, want %d", e.path, slots, mmapNodeSlotsPerExtent)
	}
	if liveSlots := e.liveSlots(); liveSlots != e.countUsed() {
		return fmt.Errorf("minweight_store: mmap node extent %s live slots = %d, bitmap count = %d", e.path, liveSlots, e.countUsed())
	}
	return nil
}

func (e *mmapNodeExtent) liveSlots() uint32 {
	return binary.LittleEndian.Uint32(e.data[28:32])
}

func (e *mmapNodeExtent) setLiveSlots(liveSlots uint32) {
	binary.LittleEndian.PutUint32(e.data[28:32], liveSlots)
}

func (e *mmapNodeExtent) alloc() (uint64, *minpatricia.NodePage, bool) {
	bitmap := e.bitmap()
	for byteIndex := 0; byteIndex < mmapNodeBitmapBytes; byteIndex++ {
		usable := mmapNodeBitmapByteMask(byteIndex)
		if bitmap[byteIndex]&usable == usable {
			continue
		}
		for bit := uint64(0); bit < 8; bit++ {
			mask := byte(1 << bit)
			if usable&mask == 0 || bitmap[byteIndex]&mask != 0 {
				continue
			}
			slot := uint64(byteIndex)*8 + bit
			// Page bytes are intentionally left untouched; the caller initializes node content.
			e.setUsed(slot, true)
			e.setLiveSlots(e.liveSlots() + 1)
			return e.id*mmapNodeSlotsPerExtent + slot, e.page(slot), true
		}
	}
	return 0, nil, false
}

func (e *mmapNodeExtent) used(slot uint64) bool {
	b := e.bitmap()[slot/8]
	mask := byte(1 << (slot % 8))
	return b&mask != 0
}

func (e *mmapNodeExtent) setUsed(slot uint64, used bool) {
	bitmap := e.bitmap()
	mask := byte(1 << (slot % 8))
	if used {
		bitmap[slot/8] |= mask
		return
	}
	bitmap[slot/8] &^= mask
}

func (e *mmapNodeExtent) countUsed() uint32 {
	var count uint32
	bitmap := e.bitmap()
	for byteIndex := 0; byteIndex < mmapNodeBitmapBytes; byteIndex++ {
		count += uint32(bits.OnesCount8(bitmap[byteIndex] & mmapNodeBitmapByteMask(byteIndex)))
	}
	return count
}

func mmapNodeBitmapByteMask(byteIndex int) byte {
	if byteIndex != mmapNodeBitmapBytes-1 || mmapNodeSlotsPerExtent%8 == 0 {
		return 0xff
	}
	return byte((1 << (mmapNodeSlotsPerExtent % 8)) - 1)
}

func (e *mmapNodeExtent) bitmap() []byte {
	return e.data[mmapNodePageSize : mmapNodePageSize*2]
}

func (e *mmapNodeExtent) page(slot uint64) *minpatricia.NodePage {
	offset := (mmapNodeReservedPages + int(slot)) * mmapNodePageSize
	return (*minpatricia.NodePage)(unsafe.Pointer(&e.data[offset]))
}

func mmapNodePageBytes(page *minpatricia.NodePage) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(page)), minpatricia.NodeSize)
}

func (e *mmapNodeExtent) sync() error {
	if err := msyncMmap(e.data); err != nil {
		return err
	}
	return e.file.Sync()
}

func (e *mmapNodeExtent) close() error {
	var firstErr error
	if e.data != nil {
		if err := msyncMmap(e.data); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := syscall.Munmap(e.data); err != nil && firstErr == nil {
			firstErr = err
		}
		e.data = nil
	}
	if e.file != nil {
		if err := e.file.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := e.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.file = nil
	}
	return firstErr
}

func (e *mmapNodeExtent) closeAfterSync() error {
	var firstErr error
	if e.data != nil {
		if err := syscall.Munmap(e.data); err != nil && firstErr == nil {
			firstErr = err
		}
		e.data = nil
	}
	if e.file != nil {
		if err := e.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.file = nil
	}
	return firstErr
}

func (e *mmapNodeExtent) destroy() error {
	var firstErr error
	if e.data != nil {
		if err := syscall.Munmap(e.data); err != nil && firstErr == nil {
			firstErr = err
		}
		e.data = nil
	}
	if e.file != nil {
		if err := e.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.file = nil
	}
	if err := os.Remove(e.path); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func msyncMmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(syscall.MS_SYNC))
	runtime.KeepAlive(data)
	if errno != 0 {
		return errno
	}
	return nil
}

func mmapNodeBitmapUsed(bitmap []byte, slot uint64) bool {
	b := bitmap[slot/8]
	mask := byte(1 << (slot % 8))
	return b&mask != 0
}
