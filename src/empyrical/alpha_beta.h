#pragma once

#include "ireturn_stat.h"
#include "stats.h"
#include "periods.h"
#include <cmath>
#include <utility>  // std::pair

namespace epoch_folio::ep {

/**
 * \class Beta
 * \brief Compute the beta of returns vs. factorReturns.
 */
    class Beta {
    public:
        explicit Beta(double riskFree=0.0)
                : m_riskFree(epoch_frame::Scalar{std::move(riskFree)}) {}

        explicit Beta(epoch_frame::Scalar riskFree)
            : m_riskFree(std::move(riskFree)) {}

        double operator()(epoch_frame::DataFrame const &frame) const {
            // Beta is Cov(returns, factorReturns) / Var(factorReturns),
            // except we adjust both by riskFree if you choose to do so.
            if (frame.size() < 2) {
                return NAN_SCALAR;
            }

            epoch_frame::Series adjReturns = frame["strategy"] - m_riskFree;
            epoch_frame::Series adjFactor = frame["benchmark"] - m_riskFree;

            auto meanAdjFactor = adjFactor.mean();
            auto meanAdjReturns = adjReturns.mean();

            // Cov(X,Y) ~ mean( (X - meanX) * (Y - meanY) )
            epoch_frame::Series factorResidual = adjFactor - meanAdjFactor;
            epoch_frame::Series returnsResidual = adjReturns - meanAdjReturns;

            // Cov
            auto covXY = (factorResidual * returnsResidual).mean();

            // Var(X) ~ mean( (X - meanX)^2 )
            auto varX = (factorResidual * factorResidual).mean();
//            if (std::fabs(varX) < 1e-30) {
//                return NAN_SCALAR;
//            }

            return (covXY / varX).as_double();
        }

    private:
        epoch_frame::Scalar m_riskFree;
    };

/**
 * \class Alpha
 * \brief Compute the annualized alpha of returns vs. factorReturns.
 *
 * alpha = ( (returns - riskFree) - beta*(factor - riskFree) ).annualizedMean
 *         i.e. the portion not explained by factor, scaled to an annual rate.
 */
    class Alpha {
    public:
        Alpha(double riskFree=0.0,
              epoch_core::EmpyricalPeriods period=epoch_core::EmpyricalPeriods::daily,
              std::optional<int> annualization=std::nullopt)
                : m_riskFree(std::move(riskFree)),
                  m_period(period),
                  m_annualization(annualization) {}

        double operator()(epoch_frame::DataFrame const& frame,
                          double knownBeta = std::numeric_limits<double>::quiet_NaN()) const
        {
            if (frame.size() < 2) {
                return NAN_SCALAR;
            }

            int annFactor = AnnualizationFactor(m_period, m_annualization);

            // If beta isn’t passed in, compute it
            epoch_frame::Scalar b{(!std::isnan(knownBeta))
                       ? knownBeta
                       : Beta(m_riskFree)(frame)};

            epoch_frame::Series adjReturns = frame["strategy"] - m_riskFree;
            epoch_frame::Series adjFactor = frame["benchmark"] - m_riskFree;

            // alpha_series = (adjReturns - b*adjFactor)
            epoch_frame::Series alphaSeries = adjReturns - (adjFactor * b);

            // The mean of alphaSeries
            auto meanAlpha = alphaSeries.mean();

            // Annualize by ( (1 + meanAlpha)^(annFactor) - 1 )
            // The Python code does something like:
            //   alpha = ( (1 + mean(alphaSeries))^annFactor ) - 1
            auto alphaVal =
                    std::pow(1.0 + meanAlpha.as_double(), static_cast<double>(annFactor)) - 1.0;
            return alphaVal;
        }

    private:
        epoch_frame::Scalar m_riskFree;
        epoch_core::EmpyricalPeriods m_period;
        std::optional<int> m_annualization;
    };


/**
 * \class AlphaBeta
 * \brief Computes both alpha and beta in one shot, returning a pair.
 */
    class AlphaBeta {
    public:
        AlphaBeta(double riskFree=0.0,
                  epoch_core::EmpyricalPeriods period= epoch_core::EmpyricalPeriods::daily,
                  std::optional<int> annualization=std::nullopt)
                : m_alpha(riskFree, period, annualization),
                  m_beta(riskFree) {}

        /**
         * \return pair<alpha, beta>
         */
        std::pair<double, double> operator()(epoch_frame::DataFrame const& frame) const
        {
            if (frame.size() < 2) {
                return {NAN_SCALAR, NAN_SCALAR};
            }

            // 1) Compute beta
            double b = m_beta(frame);

            // 2) Compute alpha using that same beta
            double a = m_alpha(frame, b);

            return {a, b};
        }

    private:
        Alpha m_alpha;
        Beta m_beta;
    };

    using RollingBeta = RollingFactorReturnsStat<Beta>;
} // namespace epoch_folio::ep
