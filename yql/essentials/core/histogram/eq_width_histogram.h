#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>
#include <util/system/unaligned_mem.h>
#include <util/generic/yexception.h>
#include <cmath>

namespace NKikimr {

// Helper functions to work with histogram values.
template <typename T>
inline T LoadFrom(const ui8* storage) {
    T val;
    std::memcpy(&val, storage, sizeof(T));
    return val;
}
template <typename T>
inline void StoreTo(ui8* storage, T value) {
    std::memcpy(storage, &value, sizeof(T));
}
template <typename T>
inline bool CmpEqual(T left, T right) {
    return left == right;
}
template <>
inline bool CmpEqual(double left, double right) {
    return std::fabs(left - right) < std::numeric_limits<double>::epsilon();
}
template <typename T>
inline bool CmpLess(T left, T right) {
    return left < right;
}

// Represents value types supported by histogram.
enum class EHistogramValueType: ui8 { Int16,
                                      Int32,
                                      Int64,
                                      Uint16,
                                      Uint32,
                                      Uint64,
                                      Double,
                                      NotSupported };

// clang-format off
#define KNOWN_FIXED_HISTOGRAM_TYPES(xx) \
xx(Int16, i16)                          \
xx(Int32, i32)                          \
xx(Int64, i64)                          \
xx(Uint16, ui16)                        \
xx(Uint32, ui32)                        \
xx(Uint64, ui64)                        \
xx(Double, double)
// clang-format on

template <typename T>
inline std::optional<EHistogramValueType> GetHistogramValueType() {
    return std::nullopt;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<i16>() {
    return EHistogramValueType::Int16;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<i32>() {
    return EHistogramValueType::Int32;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<i64>() {
    return EHistogramValueType::Int64;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<ui16>() {
    return EHistogramValueType::Uint16;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<ui32>() {
    return EHistogramValueType::Uint32;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<ui64>() {
    return EHistogramValueType::Uint64;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<double>() {
    return EHistogramValueType::Double;
}

// Bucket storage size for Equal width histogram.
constexpr const ui32 EqWidthHistogramBucketStorageSize = 8;

// This class represents an `Equal-width` histogram.
// Each bucket represents a range of contiguous values of equal width, and the
// aggregate summary stored in the bucket is the number of rows whose value lies
// within that range.
class TEqWidthHistogram {
public:
#pragma pack(push, 1)
    struct TBucket {
        // The number of values in a bucket.
        ui64 Count{0};
        // The `start` value of a bucket, the `end` of the bucket is a next start.
        // [start = start[i], end = start[i + 1])
        ui8 Start[EqWidthHistogramBucketStorageSize];
    };
    struct TBucketRange {
        ui8 Start[EqWidthHistogramBucketStorageSize];
        ui8 End[EqWidthHistogramBucketStorageSize];
    };
#pragma pack(pop)

    // Have to specify the number of buckets and type of the values.
    TEqWidthHistogram(ui64 numBuckets = 1, EHistogramValueType type = EHistogramValueType::Int32);
    // From serialized data.
    TEqWidthHistogram(const char* str, size_t size);

    void AddElement(const char* data, size_t size);

    // Returns an index of the bucket which stores the given `val`.
    // Returned index in range [0, numBuckets - 1].
    // Not using `std::lower_bound()` here because need an index to map to `suffix` and `prefix` sum.
    template <typename T>
    ui64 FindBucketIndex(T val) const {
        ui64 start = 0;
        ui64 end = GetNumBuckets() - 1;
        while (start < end) {
            auto it = start + (end - start + 1) / 2;
            if (CmpLess<T>(val, LoadFrom<T>(Buckets_[it].Start))) {
                end = it - 1;
            } else {
                start = it;
            }
        }
        return start;
    }

    // Returns a number of buckets in a histogram.
    ui64 GetNumBuckets() const {
        return Buckets_.size();
    }

    template <typename T>
    ui64 GetBucketWidth() const {
        if (ValueType_ == EHistogramValueType::NotSupported) {
            Y_ABORT("Unsupported histogram type");
        } else if (ValueType_ == EHistogramValueType::Double) {
            return 1;
        }
        Y_ASSERT(GetNumBuckets());
        if (GetNumBuckets() == 1) {
            auto val = LoadFrom<T>(Buckets_.front().Start);
            // to avoid returning zero value and casting negative values
            return val > 0 ? static_cast<ui64>(val) : 1;
        } else {
            return static_cast<ui64>(LoadFrom<T>(Buckets_[1].Start) - LoadFrom<T>(Buckets_[0].Start));
        }
    }

    // Returns histogram type.
    EHistogramValueType GetType() const {
        return ValueType_;
    }

    // Returns a number of elements in a bucket by the given `index`.
    ui64 GetNumElementsInBucket(ui64 index) const {
        Y_ASSERT(index < GetNumBuckets());
        return Buckets_[index].Count;
    }

    // Returns the start boundary value of a bucket by the given `index`.
    template <typename T>
    T GetBucketStartBoundary(ui64 index) const {
        Y_ASSERT(index < GetNumBuckets());
        return LoadFrom<T>(Buckets_[index].Start);
    }

    // Initializes buckets with a given `range`.
    template <typename T>
    void InitializeBuckets(T rangeStart, T rangeEnd) {
        TEqWidthHistogram::TBucketRange range;
        StoreTo<T>(range.Start, rangeStart);
        StoreTo<T>(range.End, rangeEnd);
        Y_ASSERT(CmpLess<T>(LoadFrom<T>(range.Start), LoadFrom<T>(range.End)));
        T rangeLen = LoadFrom<T>(range.End) - LoadFrom<T>(range.Start);
        std::memcpy(Buckets_[0].Start, range.Start, sizeof(range.Start));
        for (ui64 i = 1; i < GetNumBuckets(); ++i) {
            const T prevStart = LoadFrom<T>(Buckets_[i - 1].Start);
            StoreTo<T>(Buckets_[i].Start, prevStart + rangeLen);
        }
    }

    // Seriailizes to a binary representation
    std::pair<std::unique_ptr<char>, ui64> Serialize() const;

    void Aggregate(const TEqWidthHistogram& other);

private:
    template <typename T>
    bool BucketsEqual(const TEqWidthHistogram& other) {
        if (Buckets_.size() != other.GetNumBuckets()) {
            return false;
        } else if (this->ValueType_ != other.GetType()) {
            return false;
        } else if (GetBucketWidth<T>() != other.GetBucketWidth<T>()) {
            return false;
        }
        for (ui64 i = 0; i < Buckets_.size(); ++i) {
            if (!CmpEqual<T>(LoadFrom<T>(Buckets_[i].Start), other.GetBucketStartBoundary<T>(i))) {
                return false;
            }
        }
        return true;
    }

    // Adds the given `val` to a histogram.
    template <typename T>
    void AddElementTyped(T val) {
        const auto index = FindBucketIndex(val);
        // The given `index` in range [0, numBuckets - 1].
        const T bucketValue = LoadFrom<T>(Buckets_[index].Start);
        if (!index || (CmpEqual<T>(bucketValue, val) || CmpLess<T>(bucketValue, val))) {
            Buckets_[index].Count++;
        } else {
            Buckets_[index - 1].Count++;
        }
    }

    // Returns binary size of the histogram.
    ui64 GetBinarySize(ui64 nBuckets) const;
    EHistogramValueType ValueType_;
    TVector<TBucket> Buckets_;
};

// This class represents a machinery to estimate a value in a histogram.
class TEqWidthHistogramEstimator {
public:
    TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram);

    // Methods to estimate values.
    template <typename T>
    ui64 EstimateLessOrEqual(T val) const {
        return EstimateOrEqual<T>(val, PrefixSum_);
    }

    template <typename T>
    ui64 EstimateGreaterOrEqual(T val) const {
        return EstimateOrEqual<T>(val, SuffixSum_);
    }

    template <typename T>
    ui64 EstimateLess(T val) const {
        return EstimateNotEqual<T>(val, PrefixSum_);
    }

    template <typename T>
    ui64 EstimateGreater(T val) const {
        return EstimateNotEqual<T>(val, SuffixSum_);
    }

    template <typename T>
    ui64 EstimateEqual(T val) const {
        const auto index = Histogram_->FindBucketIndex(val);
        // Assuming uniform distribution.
        return std::max(1U, static_cast<ui32>(Histogram_->GetNumElementsInBucket(index) / Histogram_->template GetBucketWidth<T>()));
    }

    // Returns the total number elements in histogram.
    // Could be used to adjust scale.
    ui64 GetNumElements() const {
        return PrefixSum_.back();
    }

private:
    template <typename T>
    ui64 EstimateOrEqual(T val, const TVector<ui64>& sumArray) const {
        const auto index = Histogram_->FindBucketIndex(val);
        return sumArray[index];
    }

    template <typename T>
    ui64 EstimateNotEqual(T val, const TVector<ui64>& sumArray) const {
        const auto index = Histogram_->FindBucketIndex(val);
        // Take the previous backet if it's not the first one.
        if (!index) {
            return sumArray[index];
        }
        return sumArray[index - 1];
    }

    void CreatePrefixSum(ui64 numBuckets);
    void CreateSuffixSum(ui64 numBuckets);
    std::shared_ptr<TEqWidthHistogram> Histogram_;
    TVector<ui64> PrefixSum_;
    TVector<ui64> SuffixSum_;
};
} // namespace NKikimr
