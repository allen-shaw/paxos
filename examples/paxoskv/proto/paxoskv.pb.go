// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.19.4
// source: paxoskv.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type KVOperator struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key      string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value    []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Version  uint64 `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	Operator uint32 `protobuf:"varint,4,opt,name=operator,proto3" json:"operator,omitempty"`
	Sid      uint32 `protobuf:"varint,5,opt,name=sid,proto3" json:"sid,omitempty"`
}

func (x *KVOperator) Reset() {
	*x = KVOperator{}
	if protoimpl.UnsafeEnabled {
		mi := &file_paxoskv_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KVOperator) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVOperator) ProtoMessage() {}

func (x *KVOperator) ProtoReflect() protoreflect.Message {
	mi := &file_paxoskv_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KVOperator.ProtoReflect.Descriptor instead.
func (*KVOperator) Descriptor() ([]byte, []int) {
	return file_paxoskv_proto_rawDescGZIP(), []int{0}
}

func (x *KVOperator) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KVOperator) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *KVOperator) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *KVOperator) GetOperator() uint32 {
	if x != nil {
		return x.Operator
	}
	return 0
}

func (x *KVOperator) GetSid() uint32 {
	if x != nil {
		return x.Sid
	}
	return 0
}

type KVData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value     []byte `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Version   uint64 `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
	IsDeleted bool   `protobuf:"varint,3,opt,name=is_deleted,json=isDeleted,proto3" json:"is_deleted,omitempty"`
}

func (x *KVData) Reset() {
	*x = KVData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_paxoskv_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KVData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVData) ProtoMessage() {}

func (x *KVData) ProtoReflect() protoreflect.Message {
	mi := &file_paxoskv_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KVData.ProtoReflect.Descriptor instead.
func (*KVData) Descriptor() ([]byte, []int) {
	return file_paxoskv_proto_rawDescGZIP(), []int{1}
}

func (x *KVData) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *KVData) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *KVData) GetIsDeleted() bool {
	if x != nil {
		return x.IsDeleted
	}
	return false
}

type KVResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data         *KVData `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	Ret          int32   `protobuf:"varint,2,opt,name=ret,proto3" json:"ret,omitempty"`
	MasterNodeid uint64  `protobuf:"varint,3,opt,name=master_nodeid,json=masterNodeid,proto3" json:"master_nodeid,omitempty"`
}

func (x *KVResponse) Reset() {
	*x = KVResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_paxoskv_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KVResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVResponse) ProtoMessage() {}

func (x *KVResponse) ProtoReflect() protoreflect.Message {
	mi := &file_paxoskv_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KVResponse.ProtoReflect.Descriptor instead.
func (*KVResponse) Descriptor() ([]byte, []int) {
	return file_paxoskv_proto_rawDescGZIP(), []int{2}
}

func (x *KVResponse) GetData() *KVData {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *KVResponse) GetRet() int32 {
	if x != nil {
		return x.Ret
	}
	return 0
}

func (x *KVResponse) GetMasterNodeid() uint64 {
	if x != nil {
		return x.MasterNodeid
	}
	return 0
}

var File_paxoskv_proto protoreflect.FileDescriptor

var file_paxoskv_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x70, 0x61, 0x78, 0x6f, 0x73, 0x6b, 0x76, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x07, 0x70, 0x61, 0x78, 0x6f, 0x73, 0x6b, 0x76, 0x22, 0x7c, 0x0a, 0x0a, 0x4b, 0x56, 0x4f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x18,
	0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x6f, 0x70, 0x65, 0x72,
	0x61, 0x74, 0x6f, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x6f, 0x70, 0x65, 0x72,
	0x61, 0x74, 0x6f, 0x72, 0x12, 0x10, 0x0a, 0x03, 0x73, 0x69, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x0d, 0x52, 0x03, 0x73, 0x69, 0x64, 0x22, 0x57, 0x0a, 0x06, 0x4b, 0x56, 0x44, 0x61, 0x74, 0x61,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x12, 0x1d, 0x0a, 0x0a, 0x69, 0x73, 0x5f, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x09, 0x69, 0x73, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x22,
	0x68, 0x0a, 0x0a, 0x4b, 0x56, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x23, 0x0a,
	0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x70, 0x61,
	0x78, 0x6f, 0x73, 0x6b, 0x76, 0x2e, 0x4b, 0x56, 0x44, 0x61, 0x74, 0x61, 0x52, 0x04, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x10, 0x0a, 0x03, 0x72, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x03, 0x72, 0x65, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x5f, 0x6e,
	0x6f, 0x64, 0x65, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0c, 0x6d, 0x61, 0x73,
	0x74, 0x65, 0x72, 0x4e, 0x6f, 0x64, 0x65, 0x69, 0x64, 0x32, 0xe9, 0x01, 0x0a, 0x0d, 0x50, 0x61,
	0x78, 0x6f, 0x73, 0x4b, 0x56, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x12, 0x31, 0x0a, 0x03, 0x50,
	0x75, 0x74, 0x12, 0x13, 0x2e, 0x70, 0x61, 0x78, 0x6f, 0x73, 0x6b, 0x76, 0x2e, 0x4b, 0x56, 0x4f,
	0x70, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x1a, 0x13, 0x2e, 0x70, 0x61, 0x78, 0x6f, 0x73, 0x6b,
	0x76, 0x2e, 0x4b, 0x56, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x36,
	0x0a, 0x08, 0x47, 0x65, 0x74, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x12, 0x13, 0x2e, 0x70, 0x61, 0x78,
	0x6f, 0x73, 0x6b, 0x76, 0x2e, 0x4b, 0x56, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x1a,
	0x13, 0x2e, 0x70, 0x61, 0x78, 0x6f, 0x73, 0x6b, 0x76, 0x2e, 0x4b, 0x56, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x37, 0x0a, 0x09, 0x47, 0x65, 0x74, 0x47, 0x6c, 0x6f,
	0x62, 0x61, 0x6c, 0x12, 0x13, 0x2e, 0x70, 0x61, 0x78, 0x6f, 0x73, 0x6b, 0x76, 0x2e, 0x4b, 0x56,
	0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x1a, 0x13, 0x2e, 0x70, 0x61, 0x78, 0x6f, 0x73,
	0x6b, 0x76, 0x2e, 0x4b, 0x56, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12,
	0x34, 0x0a, 0x06, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x13, 0x2e, 0x70, 0x61, 0x78, 0x6f,
	0x73, 0x6b, 0x76, 0x2e, 0x4b, 0x56, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x1a, 0x13,
	0x2e, 0x70, 0x61, 0x78, 0x6f, 0x73, 0x6b, 0x76, 0x2e, 0x4b, 0x56, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x3a, 0x5a, 0x38, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e,
	0x63, 0x6f, 0x6d, 0x2f, 0x41, 0x6c, 0x6c, 0x65, 0x6e, 0x53, 0x68, 0x61, 0x77, 0x31, 0x39, 0x2f,
	0x70, 0x61, 0x78, 0x6f, 0x73, 0x2f, 0x65, 0x78, 0x6d, 0x61, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61,
	0x78, 0x6f, 0x73, 0x6b, 0x76, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x3b, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_paxoskv_proto_rawDescOnce sync.Once
	file_paxoskv_proto_rawDescData = file_paxoskv_proto_rawDesc
)

func file_paxoskv_proto_rawDescGZIP() []byte {
	file_paxoskv_proto_rawDescOnce.Do(func() {
		file_paxoskv_proto_rawDescData = protoimpl.X.CompressGZIP(file_paxoskv_proto_rawDescData)
	})
	return file_paxoskv_proto_rawDescData
}

var file_paxoskv_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_paxoskv_proto_goTypes = []interface{}{
	(*KVOperator)(nil), // 0: paxoskv.KVOperator
	(*KVData)(nil),     // 1: paxoskv.KVData
	(*KVResponse)(nil), // 2: paxoskv.KVResponse
}
var file_paxoskv_proto_depIdxs = []int32{
	1, // 0: paxoskv.KVResponse.data:type_name -> paxoskv.KVData
	0, // 1: paxoskv.PaxosKVServer.Put:input_type -> paxoskv.KVOperator
	0, // 2: paxoskv.PaxosKVServer.GetLocal:input_type -> paxoskv.KVOperator
	0, // 3: paxoskv.PaxosKVServer.GetGlobal:input_type -> paxoskv.KVOperator
	0, // 4: paxoskv.PaxosKVServer.Delete:input_type -> paxoskv.KVOperator
	2, // 5: paxoskv.PaxosKVServer.Put:output_type -> paxoskv.KVResponse
	2, // 6: paxoskv.PaxosKVServer.GetLocal:output_type -> paxoskv.KVResponse
	2, // 7: paxoskv.PaxosKVServer.GetGlobal:output_type -> paxoskv.KVResponse
	2, // 8: paxoskv.PaxosKVServer.Delete:output_type -> paxoskv.KVResponse
	5, // [5:9] is the sub-list for method output_type
	1, // [1:5] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_paxoskv_proto_init() }
func file_paxoskv_proto_init() {
	if File_paxoskv_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_paxoskv_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KVOperator); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_paxoskv_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KVData); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_paxoskv_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KVResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_paxoskv_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_paxoskv_proto_goTypes,
		DependencyIndexes: file_paxoskv_proto_depIdxs,
		MessageInfos:      file_paxoskv_proto_msgTypes,
	}.Build()
	File_paxoskv_proto = out.File
	file_paxoskv_proto_rawDesc = nil
	file_paxoskv_proto_goTypes = nil
	file_paxoskv_proto_depIdxs = nil
}
