// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: snippet_sample.proto

#include "snippet_sample.pb.h"

#include <algorithm>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)
#include <google/protobuf/port_def.inc>

PROTOBUF_PRAGMA_INIT_SEG
namespace snippetsample {
constexpr Snippet::Snippet(
  ::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized)
  : snippet_(&::PROTOBUF_NAMESPACE_ID::internal::fixed_address_empty_string)
  , queryid_(0)
  , workid_(0){}
struct SnippetDefaultTypeInternal {
  constexpr SnippetDefaultTypeInternal()
    : _instance(::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized{}) {}
  ~SnippetDefaultTypeInternal() {}
  union {
    Snippet _instance;
  };
};
PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT SnippetDefaultTypeInternal _Snippet_default_instance_;
constexpr Request::Request(
  ::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized)
  : queryid_(0){}
struct RequestDefaultTypeInternal {
  constexpr RequestDefaultTypeInternal()
    : _instance(::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized{}) {}
  ~RequestDefaultTypeInternal() {}
  union {
    Request _instance;
  };
};
PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT RequestDefaultTypeInternal _Request_default_instance_;
constexpr Result::Result(
  ::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized)
  : value_(&::PROTOBUF_NAMESPACE_ID::internal::fixed_address_empty_string){}
struct ResultDefaultTypeInternal {
  constexpr ResultDefaultTypeInternal()
    : _instance(::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized{}) {}
  ~ResultDefaultTypeInternal() {}
  union {
    Result _instance;
  };
};
PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT ResultDefaultTypeInternal _Result_default_instance_;
}  // namespace snippetsample
static ::PROTOBUF_NAMESPACE_ID::Metadata file_level_metadata_snippet_5fsample_2eproto[3];
static constexpr ::PROTOBUF_NAMESPACE_ID::EnumDescriptor const** file_level_enum_descriptors_snippet_5fsample_2eproto = nullptr;
static constexpr ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor const** file_level_service_descriptors_snippet_5fsample_2eproto = nullptr;

const uint32_t TableStruct_snippet_5fsample_2eproto::offsets[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::snippetsample::Snippet, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  ~0u,  // no _inlined_string_donated_
  PROTOBUF_FIELD_OFFSET(::snippetsample::Snippet, queryid_),
  PROTOBUF_FIELD_OFFSET(::snippetsample::Snippet, workid_),
  PROTOBUF_FIELD_OFFSET(::snippetsample::Snippet, snippet_),
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::snippetsample::Request, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  ~0u,  // no _inlined_string_donated_
  PROTOBUF_FIELD_OFFSET(::snippetsample::Request, queryid_),
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::snippetsample::Result, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  ~0u,  // no _inlined_string_donated_
  PROTOBUF_FIELD_OFFSET(::snippetsample::Result, value_),
};
static const ::PROTOBUF_NAMESPACE_ID::internal::MigrationSchema schemas[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  { 0, -1, -1, sizeof(::snippetsample::Snippet)},
  { 9, -1, -1, sizeof(::snippetsample::Request)},
  { 16, -1, -1, sizeof(::snippetsample::Result)},
};

static ::PROTOBUF_NAMESPACE_ID::Message const * const file_default_instances[] = {
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::snippetsample::_Snippet_default_instance_),
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::snippetsample::_Request_default_instance_),
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::snippetsample::_Result_default_instance_),
};

const char descriptor_table_protodef_snippet_5fsample_2eproto[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) =
  "\n\024snippet_sample.proto\022\rsnippetsample\032\033g"
  "oogle/protobuf/empty.proto\";\n\007Snippet\022\017\n"
  "\007queryid\030\001 \001(\005\022\016\n\006workid\030\002 \001(\005\022\017\n\007snippe"
  "t\030\003 \001(\t\"\032\n\007Request\022\017\n\007queryid\030\001 \001(\005\"\027\n\006R"
  "esult\022\r\n\005value\030\001 \001(\t2\213\001\n\rSnippetSample\022B"
  "\n\nSetSnippet\022\026.snippetsample.Snippet\032\026.g"
  "oogle.protobuf.Empty\"\000(\0010\001\0226\n\003Run\022\026.snip"
  "petsample.Request\032\025.snippetsample.Result"
  "\"\000B6\n\026io.grpc.snippet_sampleB\024snippet_sa"
  "mple_ProtoP\001\242\002\003SSPb\006proto3"
  ;
static const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable*const descriptor_table_snippet_5fsample_2eproto_deps[1] = {
  &::descriptor_table_google_2fprotobuf_2fempty_2eproto,
};
static ::PROTOBUF_NAMESPACE_ID::internal::once_flag descriptor_table_snippet_5fsample_2eproto_once;
const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable descriptor_table_snippet_5fsample_2eproto = {
  false, false, 386, descriptor_table_protodef_snippet_5fsample_2eproto, "snippet_sample.proto", 
  &descriptor_table_snippet_5fsample_2eproto_once, descriptor_table_snippet_5fsample_2eproto_deps, 1, 3,
  schemas, file_default_instances, TableStruct_snippet_5fsample_2eproto::offsets,
  file_level_metadata_snippet_5fsample_2eproto, file_level_enum_descriptors_snippet_5fsample_2eproto, file_level_service_descriptors_snippet_5fsample_2eproto,
};
PROTOBUF_ATTRIBUTE_WEAK const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable* descriptor_table_snippet_5fsample_2eproto_getter() {
  return &descriptor_table_snippet_5fsample_2eproto;
}

// Force running AddDescriptors() at dynamic initialization time.
PROTOBUF_ATTRIBUTE_INIT_PRIORITY static ::PROTOBUF_NAMESPACE_ID::internal::AddDescriptorsRunner dynamic_init_dummy_snippet_5fsample_2eproto(&descriptor_table_snippet_5fsample_2eproto);
namespace snippetsample {

// ===================================================================

class Snippet::_Internal {
 public:
};

Snippet::Snippet(::PROTOBUF_NAMESPACE_ID::Arena* arena,
                         bool is_message_owned)
  : ::PROTOBUF_NAMESPACE_ID::Message(arena, is_message_owned) {
  SharedCtor();
  if (!is_message_owned) {
    RegisterArenaDtor(arena);
  }
  // @@protoc_insertion_point(arena_constructor:snippetsample.Snippet)
}
Snippet::Snippet(const Snippet& from)
  : ::PROTOBUF_NAMESPACE_ID::Message() {
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
  snippet_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  #ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
    snippet_.Set(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), "", GetArenaForAllocation());
  #endif // PROTOBUF_FORCE_COPY_DEFAULT_STRING
  if (!from._internal_snippet().empty()) {
    snippet_.Set(::PROTOBUF_NAMESPACE_ID::internal::ArenaStringPtr::EmptyDefault{}, from._internal_snippet(), 
      GetArenaForAllocation());
  }
  ::memcpy(&queryid_, &from.queryid_,
    static_cast<size_t>(reinterpret_cast<char*>(&workid_) -
    reinterpret_cast<char*>(&queryid_)) + sizeof(workid_));
  // @@protoc_insertion_point(copy_constructor:snippetsample.Snippet)
}

inline void Snippet::SharedCtor() {
snippet_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
#ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
  snippet_.Set(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), "", GetArenaForAllocation());
#endif // PROTOBUF_FORCE_COPY_DEFAULT_STRING
::memset(reinterpret_cast<char*>(this) + static_cast<size_t>(
    reinterpret_cast<char*>(&queryid_) - reinterpret_cast<char*>(this)),
    0, static_cast<size_t>(reinterpret_cast<char*>(&workid_) -
    reinterpret_cast<char*>(&queryid_)) + sizeof(workid_));
}

Snippet::~Snippet() {
  // @@protoc_insertion_point(destructor:snippetsample.Snippet)
  if (GetArenaForAllocation() != nullptr) return;
  SharedDtor();
  _internal_metadata_.Delete<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

inline void Snippet::SharedDtor() {
  GOOGLE_DCHECK(GetArenaForAllocation() == nullptr);
  snippet_.DestroyNoArena(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
}

void Snippet::ArenaDtor(void* object) {
  Snippet* _this = reinterpret_cast< Snippet* >(object);
  (void)_this;
}
void Snippet::RegisterArenaDtor(::PROTOBUF_NAMESPACE_ID::Arena*) {
}
void Snippet::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}

void Snippet::Clear() {
// @@protoc_insertion_point(message_clear_start:snippetsample.Snippet)
  uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  snippet_.ClearToEmpty();
  ::memset(&queryid_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&workid_) -
      reinterpret_cast<char*>(&queryid_)) + sizeof(workid_));
  _internal_metadata_.Clear<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

const char* Snippet::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  while (!ctx->Done(&ptr)) {
    uint32_t tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    switch (tag >> 3) {
      // int32 queryid = 1;
      case 1:
        if (PROTOBUF_PREDICT_TRUE(static_cast<uint8_t>(tag) == 8)) {
          queryid_ = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint32(&ptr);
          CHK_(ptr);
        } else
          goto handle_unusual;
        continue;
      // int32 workid = 2;
      case 2:
        if (PROTOBUF_PREDICT_TRUE(static_cast<uint8_t>(tag) == 16)) {
          workid_ = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint32(&ptr);
          CHK_(ptr);
        } else
          goto handle_unusual;
        continue;
      // string snippet = 3;
      case 3:
        if (PROTOBUF_PREDICT_TRUE(static_cast<uint8_t>(tag) == 26)) {
          auto str = _internal_mutable_snippet();
          ptr = ::PROTOBUF_NAMESPACE_ID::internal::InlineGreedyStringParser(str, ptr, ctx);
          CHK_(::PROTOBUF_NAMESPACE_ID::internal::VerifyUTF8(str, "snippetsample.Snippet.snippet"));
          CHK_(ptr);
        } else
          goto handle_unusual;
        continue;
      default:
        goto handle_unusual;
    }  // switch
  handle_unusual:
    if ((tag == 0) || ((tag & 7) == 4)) {
      CHK_(ptr);
      ctx->SetLastTag(tag);
      goto message_done;
    }
    ptr = UnknownFieldParse(
        tag,
        _internal_metadata_.mutable_unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(),
        ptr, ctx);
    CHK_(ptr != nullptr);
  }  // while
message_done:
  return ptr;
failure:
  ptr = nullptr;
  goto message_done;
#undef CHK_
}

uint8_t* Snippet::_InternalSerialize(
    uint8_t* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:snippetsample.Snippet)
  uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  // int32 queryid = 1;
  if (this->_internal_queryid() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteInt32ToArray(1, this->_internal_queryid(), target);
  }

  // int32 workid = 2;
  if (this->_internal_workid() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteInt32ToArray(2, this->_internal_workid(), target);
  }

  // string snippet = 3;
  if (!this->_internal_snippet().empty()) {
    ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::VerifyUtf8String(
      this->_internal_snippet().data(), static_cast<int>(this->_internal_snippet().length()),
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::SERIALIZE,
      "snippetsample.Snippet.snippet");
    target = stream->WriteStringMaybeAliased(
        3, this->_internal_snippet(), target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(::PROTOBUF_NAMESPACE_ID::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:snippetsample.Snippet)
  return target;
}

size_t Snippet::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:snippetsample.Snippet)
  size_t total_size = 0;

  uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // string snippet = 3;
  if (!this->_internal_snippet().empty()) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::StringSize(
        this->_internal_snippet());
  }

  // int32 queryid = 1;
  if (this->_internal_queryid() != 0) {
    total_size += ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::Int32SizePlusOne(this->_internal_queryid());
  }

  // int32 workid = 2;
  if (this->_internal_workid() != 0) {
    total_size += ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::Int32SizePlusOne(this->_internal_workid());
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_cached_size_);
}

const ::PROTOBUF_NAMESPACE_ID::Message::ClassData Snippet::_class_data_ = {
    ::PROTOBUF_NAMESPACE_ID::Message::CopyWithSizeCheck,
    Snippet::MergeImpl
};
const ::PROTOBUF_NAMESPACE_ID::Message::ClassData*Snippet::GetClassData() const { return &_class_data_; }

void Snippet::MergeImpl(::PROTOBUF_NAMESPACE_ID::Message* to,
                      const ::PROTOBUF_NAMESPACE_ID::Message& from) {
  static_cast<Snippet *>(to)->MergeFrom(
      static_cast<const Snippet &>(from));
}


void Snippet::MergeFrom(const Snippet& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:snippetsample.Snippet)
  GOOGLE_DCHECK_NE(&from, this);
  uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  if (!from._internal_snippet().empty()) {
    _internal_set_snippet(from._internal_snippet());
  }
  if (from._internal_queryid() != 0) {
    _internal_set_queryid(from._internal_queryid());
  }
  if (from._internal_workid() != 0) {
    _internal_set_workid(from._internal_workid());
  }
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
}

void Snippet::CopyFrom(const Snippet& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:snippetsample.Snippet)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool Snippet::IsInitialized() const {
  return true;
}

void Snippet::InternalSwap(Snippet* other) {
  using std::swap;
  auto* lhs_arena = GetArenaForAllocation();
  auto* rhs_arena = other->GetArenaForAllocation();
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::internal::ArenaStringPtr::InternalSwap(
      &::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(),
      &snippet_, lhs_arena,
      &other->snippet_, rhs_arena
  );
  ::PROTOBUF_NAMESPACE_ID::internal::memswap<
      PROTOBUF_FIELD_OFFSET(Snippet, workid_)
      + sizeof(Snippet::workid_)
      - PROTOBUF_FIELD_OFFSET(Snippet, queryid_)>(
          reinterpret_cast<char*>(&queryid_),
          reinterpret_cast<char*>(&other->queryid_));
}

::PROTOBUF_NAMESPACE_ID::Metadata Snippet::GetMetadata() const {
  return ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(
      &descriptor_table_snippet_5fsample_2eproto_getter, &descriptor_table_snippet_5fsample_2eproto_once,
      file_level_metadata_snippet_5fsample_2eproto[0]);
}

// ===================================================================

class Request::_Internal {
 public:
};

Request::Request(::PROTOBUF_NAMESPACE_ID::Arena* arena,
                         bool is_message_owned)
  : ::PROTOBUF_NAMESPACE_ID::Message(arena, is_message_owned) {
  SharedCtor();
  if (!is_message_owned) {
    RegisterArenaDtor(arena);
  }
  // @@protoc_insertion_point(arena_constructor:snippetsample.Request)
}
Request::Request(const Request& from)
  : ::PROTOBUF_NAMESPACE_ID::Message() {
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
  queryid_ = from.queryid_;
  // @@protoc_insertion_point(copy_constructor:snippetsample.Request)
}

inline void Request::SharedCtor() {
queryid_ = 0;
}

Request::~Request() {
  // @@protoc_insertion_point(destructor:snippetsample.Request)
  if (GetArenaForAllocation() != nullptr) return;
  SharedDtor();
  _internal_metadata_.Delete<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

inline void Request::SharedDtor() {
  GOOGLE_DCHECK(GetArenaForAllocation() == nullptr);
}

void Request::ArenaDtor(void* object) {
  Request* _this = reinterpret_cast< Request* >(object);
  (void)_this;
}
void Request::RegisterArenaDtor(::PROTOBUF_NAMESPACE_ID::Arena*) {
}
void Request::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}

void Request::Clear() {
// @@protoc_insertion_point(message_clear_start:snippetsample.Request)
  uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  queryid_ = 0;
  _internal_metadata_.Clear<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

const char* Request::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  while (!ctx->Done(&ptr)) {
    uint32_t tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    switch (tag >> 3) {
      // int32 queryid = 1;
      case 1:
        if (PROTOBUF_PREDICT_TRUE(static_cast<uint8_t>(tag) == 8)) {
          queryid_ = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint32(&ptr);
          CHK_(ptr);
        } else
          goto handle_unusual;
        continue;
      default:
        goto handle_unusual;
    }  // switch
  handle_unusual:
    if ((tag == 0) || ((tag & 7) == 4)) {
      CHK_(ptr);
      ctx->SetLastTag(tag);
      goto message_done;
    }
    ptr = UnknownFieldParse(
        tag,
        _internal_metadata_.mutable_unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(),
        ptr, ctx);
    CHK_(ptr != nullptr);
  }  // while
message_done:
  return ptr;
failure:
  ptr = nullptr;
  goto message_done;
#undef CHK_
}

uint8_t* Request::_InternalSerialize(
    uint8_t* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:snippetsample.Request)
  uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  // int32 queryid = 1;
  if (this->_internal_queryid() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteInt32ToArray(1, this->_internal_queryid(), target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(::PROTOBUF_NAMESPACE_ID::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:snippetsample.Request)
  return target;
}

size_t Request::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:snippetsample.Request)
  size_t total_size = 0;

  uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // int32 queryid = 1;
  if (this->_internal_queryid() != 0) {
    total_size += ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::Int32SizePlusOne(this->_internal_queryid());
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_cached_size_);
}

const ::PROTOBUF_NAMESPACE_ID::Message::ClassData Request::_class_data_ = {
    ::PROTOBUF_NAMESPACE_ID::Message::CopyWithSizeCheck,
    Request::MergeImpl
};
const ::PROTOBUF_NAMESPACE_ID::Message::ClassData*Request::GetClassData() const { return &_class_data_; }

void Request::MergeImpl(::PROTOBUF_NAMESPACE_ID::Message* to,
                      const ::PROTOBUF_NAMESPACE_ID::Message& from) {
  static_cast<Request *>(to)->MergeFrom(
      static_cast<const Request &>(from));
}


void Request::MergeFrom(const Request& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:snippetsample.Request)
  GOOGLE_DCHECK_NE(&from, this);
  uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  if (from._internal_queryid() != 0) {
    _internal_set_queryid(from._internal_queryid());
  }
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
}

void Request::CopyFrom(const Request& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:snippetsample.Request)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool Request::IsInitialized() const {
  return true;
}

void Request::InternalSwap(Request* other) {
  using std::swap;
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  swap(queryid_, other->queryid_);
}

::PROTOBUF_NAMESPACE_ID::Metadata Request::GetMetadata() const {
  return ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(
      &descriptor_table_snippet_5fsample_2eproto_getter, &descriptor_table_snippet_5fsample_2eproto_once,
      file_level_metadata_snippet_5fsample_2eproto[1]);
}

// ===================================================================

class Result::_Internal {
 public:
};

Result::Result(::PROTOBUF_NAMESPACE_ID::Arena* arena,
                         bool is_message_owned)
  : ::PROTOBUF_NAMESPACE_ID::Message(arena, is_message_owned) {
  SharedCtor();
  if (!is_message_owned) {
    RegisterArenaDtor(arena);
  }
  // @@protoc_insertion_point(arena_constructor:snippetsample.Result)
}
Result::Result(const Result& from)
  : ::PROTOBUF_NAMESPACE_ID::Message() {
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
  value_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  #ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
    value_.Set(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), "", GetArenaForAllocation());
  #endif // PROTOBUF_FORCE_COPY_DEFAULT_STRING
  if (!from._internal_value().empty()) {
    value_.Set(::PROTOBUF_NAMESPACE_ID::internal::ArenaStringPtr::EmptyDefault{}, from._internal_value(), 
      GetArenaForAllocation());
  }
  // @@protoc_insertion_point(copy_constructor:snippetsample.Result)
}

inline void Result::SharedCtor() {
value_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
#ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
  value_.Set(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), "", GetArenaForAllocation());
#endif // PROTOBUF_FORCE_COPY_DEFAULT_STRING
}

Result::~Result() {
  // @@protoc_insertion_point(destructor:snippetsample.Result)
  if (GetArenaForAllocation() != nullptr) return;
  SharedDtor();
  _internal_metadata_.Delete<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

inline void Result::SharedDtor() {
  GOOGLE_DCHECK(GetArenaForAllocation() == nullptr);
  value_.DestroyNoArena(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
}

void Result::ArenaDtor(void* object) {
  Result* _this = reinterpret_cast< Result* >(object);
  (void)_this;
}
void Result::RegisterArenaDtor(::PROTOBUF_NAMESPACE_ID::Arena*) {
}
void Result::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}

void Result::Clear() {
// @@protoc_insertion_point(message_clear_start:snippetsample.Result)
  uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  value_.ClearToEmpty();
  _internal_metadata_.Clear<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

const char* Result::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  while (!ctx->Done(&ptr)) {
    uint32_t tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    switch (tag >> 3) {
      // string value = 1;
      case 1:
        if (PROTOBUF_PREDICT_TRUE(static_cast<uint8_t>(tag) == 10)) {
          auto str = _internal_mutable_value();
          ptr = ::PROTOBUF_NAMESPACE_ID::internal::InlineGreedyStringParser(str, ptr, ctx);
          CHK_(::PROTOBUF_NAMESPACE_ID::internal::VerifyUTF8(str, "snippetsample.Result.value"));
          CHK_(ptr);
        } else
          goto handle_unusual;
        continue;
      default:
        goto handle_unusual;
    }  // switch
  handle_unusual:
    if ((tag == 0) || ((tag & 7) == 4)) {
      CHK_(ptr);
      ctx->SetLastTag(tag);
      goto message_done;
    }
    ptr = UnknownFieldParse(
        tag,
        _internal_metadata_.mutable_unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(),
        ptr, ctx);
    CHK_(ptr != nullptr);
  }  // while
message_done:
  return ptr;
failure:
  ptr = nullptr;
  goto message_done;
#undef CHK_
}

uint8_t* Result::_InternalSerialize(
    uint8_t* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:snippetsample.Result)
  uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  // string value = 1;
  if (!this->_internal_value().empty()) {
    ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::VerifyUtf8String(
      this->_internal_value().data(), static_cast<int>(this->_internal_value().length()),
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::SERIALIZE,
      "snippetsample.Result.value");
    target = stream->WriteStringMaybeAliased(
        1, this->_internal_value(), target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(::PROTOBUF_NAMESPACE_ID::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:snippetsample.Result)
  return target;
}

size_t Result::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:snippetsample.Result)
  size_t total_size = 0;

  uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // string value = 1;
  if (!this->_internal_value().empty()) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::StringSize(
        this->_internal_value());
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_cached_size_);
}

const ::PROTOBUF_NAMESPACE_ID::Message::ClassData Result::_class_data_ = {
    ::PROTOBUF_NAMESPACE_ID::Message::CopyWithSizeCheck,
    Result::MergeImpl
};
const ::PROTOBUF_NAMESPACE_ID::Message::ClassData*Result::GetClassData() const { return &_class_data_; }

void Result::MergeImpl(::PROTOBUF_NAMESPACE_ID::Message* to,
                      const ::PROTOBUF_NAMESPACE_ID::Message& from) {
  static_cast<Result *>(to)->MergeFrom(
      static_cast<const Result &>(from));
}


void Result::MergeFrom(const Result& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:snippetsample.Result)
  GOOGLE_DCHECK_NE(&from, this);
  uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  if (!from._internal_value().empty()) {
    _internal_set_value(from._internal_value());
  }
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
}

void Result::CopyFrom(const Result& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:snippetsample.Result)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool Result::IsInitialized() const {
  return true;
}

void Result::InternalSwap(Result* other) {
  using std::swap;
  auto* lhs_arena = GetArenaForAllocation();
  auto* rhs_arena = other->GetArenaForAllocation();
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::internal::ArenaStringPtr::InternalSwap(
      &::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(),
      &value_, lhs_arena,
      &other->value_, rhs_arena
  );
}

::PROTOBUF_NAMESPACE_ID::Metadata Result::GetMetadata() const {
  return ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(
      &descriptor_table_snippet_5fsample_2eproto_getter, &descriptor_table_snippet_5fsample_2eproto_once,
      file_level_metadata_snippet_5fsample_2eproto[2]);
}

// @@protoc_insertion_point(namespace_scope)
}  // namespace snippetsample
PROTOBUF_NAMESPACE_OPEN
template<> PROTOBUF_NOINLINE ::snippetsample::Snippet* Arena::CreateMaybeMessage< ::snippetsample::Snippet >(Arena* arena) {
  return Arena::CreateMessageInternal< ::snippetsample::Snippet >(arena);
}
template<> PROTOBUF_NOINLINE ::snippetsample::Request* Arena::CreateMaybeMessage< ::snippetsample::Request >(Arena* arena) {
  return Arena::CreateMessageInternal< ::snippetsample::Request >(arena);
}
template<> PROTOBUF_NOINLINE ::snippetsample::Result* Arena::CreateMaybeMessage< ::snippetsample::Result >(Arena* arena) {
  return Arena::CreateMessageInternal< ::snippetsample::Result >(arena);
}
PROTOBUF_NAMESPACE_CLOSE

// @@protoc_insertion_point(global_scope)
#include <google/protobuf/port_undef.inc>
