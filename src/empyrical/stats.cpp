//
// Created by adesola on 1/6/25.
//
#include "stats.h"
#include <valarray>
#include <epoch_frame/index.h>
#include <epoch_frame/factory/date_offset_factory.h>

namespace epoch_folio::ep
{
    using namespace epoch_frame;
    using namespace epoch_core;

    Series AggregateReturns(Series const &returns, EmpyricalPeriods convertTo)
    {
        auto cumulateReturns = [](DataFrame const &x) -> epoch_frame::Scalar
        {
            return CumReturns(x.to_series()).iloc(-1);
        };

        arrow::ChunkedArrayVector groupings;
        auto index_array = returns.index()->array().dt();
        groupings.emplace_back(
            std::make_shared<arrow::ChunkedArray>(index_array.year().value())
        );

        switch (convertTo)
        {
            case EmpyricalPeriods::weekly:
                groupings.emplace_back(
                std::make_shared<arrow::ChunkedArray>(index_array.iso_week().value())
                );
                break;
            case EmpyricalPeriods::monthly:
                groupings.emplace_back(
                std::make_shared<arrow::ChunkedArray>(index_array.month().value())
                );
                break;
            case EmpyricalPeriods::quarterly:
                groupings.emplace_back(
                std::make_shared<arrow::ChunkedArray>(index_array.quarter().value())
                );
                break;
            case EmpyricalPeriods::yearly:
                break;
            default:
                throw std::runtime_error("convertTo must be weekly, monthly, yearly. not " +
                                         EmpyricalPeriodsWrapper::ToString(convertTo));
        }

        return returns.to_frame().group_by_apply(groupings).apply(cumulateReturns);
    }

    Series CumReturns(Series const &returns, double startingValue)
    {
        if (returns.size() < 1)
            return returns;

        const Scalar one{1.0};
        const Scalar zero{0.0};
        auto out = returns.where(returns.is_valid(), zero);

        out = out + one;
        return startingValue == 0 ? (out.cumulative_prod() - one) : out.cumulative_prod(true, startingValue);
    }

    double CumReturnsFinal(epoch_frame::Series const &returns, double startingValue)
    {
        if (returns.size() < 1)
            return NAN_SCALAR;

        const Scalar one{1.0};
        auto result = (returns + one).product().as_double();
        return startingValue == 0 ? (result - 1.0) : (result * startingValue);
    }

    Series DrawDownSeries(Series const &returns)
    {
        if (returns.size() == 0)
        {
            return returns;
        }

        double start = 100;
        Series out = CumReturns(returns, start);
        auto maxReturn = out.cumulative_max(true, start);

        return (out - maxReturn) / maxReturn;
    }

    double Moment(epoch_frame::Series const &data,
                  int order,
                  double meanVal)
    {
        // 1) Handle empty data
        size_t n = data.size();
        if (n == 0)
        {
            // By convention, moment of empty array => mean of empty => NaN
            return NAN_SCALAR;
        }

        // 2) If order == 0 => always 1
        if (order == 0)
        {
            return 1.0;
        }

        // 3) If order == 1 and meanVal not provided => 0
        //    (the first central moment about its own mean is always 0)
        bool hasMean = !std::isnan(meanVal);
        if (order == 1 && !hasMean)
        {
            return 0.0;
        }

        // 4) Drop NaNs if your epoch_frame::Series has that method
        const epoch_frame::Series cleaned = data.drop_null();
        n = cleaned.size();
        if (n == 0)
        {
            return NAN_SCALAR;
        }

        // 5) If meanVal is NaN, compute from cleaned data
        if (!hasMean)
        {
            meanVal = cleaned.mean().as_double();
            hasMean = true;
        }

        // Create a container of (x - mean)
        std::vector<double> a_zero_mean;
        a_zero_mean.reserve(n);
        std::shared_ptr<arrow::DoubleArray> double_array = cleaned.contiguous_array().to_view<double>();
        AssertFromFormat(double_array, "double_array is null");
        for (auto const &val_or_null : *double_array)
        {
            a_zero_mean.push_back(val_or_null.value_or(NAN_SCALAR) - meanVal);
        }

        // 6) Check for potential catastrophic cancellation:
        //    max(|x - mean|) / |mean| < some small epsilon => potential precision issues
        //    Only if mean != 0, of course.
        double absMean = std::fabs(meanVal);
        double maxDiff = 0.0;
        for (double diff : a_zero_mean)
        {
            double ad = std::fabs(diff);
            if (ad > maxDiff)
            {
                maxDiff = ad;
            }
        }

        // We mimic Python's approach: eps * 10, for instance
        double eps = std::numeric_limits<double>::epsilon() * 10.0;
        if (absMean > 1e-30)
        {
            double relDiff = maxDiff / absMean;
            if ((relDiff < eps) && (n > 1))
            {
                std::cerr << "Warning: Precision loss in moment calculation. Data nearly identical.\n";
            }
        }

        // 7) If order == 1 and mean provided => sum of (x - mean) / n
        if (order == 1)
        {
            // moment_1 = average of (x - mean)
            // But about the same mean => 0, theoretically.
            // We do it anyway for completeness:
            double sum1 = 0.0;
            for (double diff : a_zero_mean)
            {
                sum1 += diff;
            }
            return sum1 / static_cast<double>(n);
        }

        // 8) Exponentiation by squaring
        //    We'll build n_list, the sequence of exponents from 'order' down to 1
        //    then start from (x - mean)^1 or ^2, repeatedly squaring.
        //    This is a direct port of the Python logic.
        std::vector<int> n_list;
        {
            int current = order;
            n_list.push_back(current);
            while (current > 2)
            {
                if (current % 2 == 1)
                {
                    current = (current - 1) / 2;
                }
                else
                {
                    current /= 2;
                }
                n_list.push_back(current);
            }
        }

        // We'll store our partial computations in a std::vector<double> s
        // Start with either (x - mean) or (x - mean)^2
        std::vector<double> s(n);
        int last = n_list.back();
        if (last == 1)
        {
            // s = a_zero_mean
            s = a_zero_mean;
        }
        else
        {
            // last == 2
            // s = (a_zero_mean)^2
            for (size_t i = 0; i < n; ++i)
            {
                double d = a_zero_mean[i];
                s[i] = d * d;
            }
        }

        // Iterate from the second-to-last exponent in n_list down to the beginning
        // For each exponent 'exp':
        //   s = s^2
        //   if exp is odd => s *= a_zero_mean
        for (int i = static_cast<int>(n_list.size()) - 2; i >= 0; --i)
        {
            int expVal = n_list[i];
            // s = s^2
            for (size_t k = 0; k < n; ++k)
            {
                s[k] = s[k] * s[k];
            }
            // if expVal is odd => multiply by (x - mean)
            if (expVal % 2 == 1)
            {
                for (size_t k = 0; k < n; ++k)
                {
                    s[k] *= a_zero_mean[k];
                }
            }
        }

        // 9) Take the mean of s => the central moment
        double sumPow = 0.0;
        for (auto val : s)
        {
            sumPow += val;
        }
        double result = sumPow / static_cast<double>(n);

        return result;
    }

    double RValue(Array const &x, Array const &y)
    {
        const auto ssxym = ((x - x.mean()) * (y - y.mean())).mean().as_double();
        const auto ssxm = ((x - x.mean()).pow(2_scalar)).mean().as_double();
        const auto ssym = ((y - y.mean()).pow(2_scalar)).mean().as_double();
        if (ssxm == 0.0 or ssym == 0.0)
        {
            return 0.0;
        }

        const auto r = ssxym / std::sqrt(ssxm * ssym);
        return std::clamp(r, -1.0, 1.0);
    }
}