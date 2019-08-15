#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "envoy/buffer/buffer.h"
#include "envoy/config/filter/network/kafka/v1alpha1/kafka_simple.pb.h"
#include "envoy/config/filter/network/kafka/v1alpha1/kafka_simple.pb.validate.h"
#include "envoy/network/filter.h"
#include "envoy/server/filter_config.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/stats/symbol_table.h"

#include "common/common/fmt.h"
#include "common/common/logger.h"
#include "common/stats/symbol_table_impl.h"
#include "extensions/filters/network/common/factory_base.h"
#include "extensions/filters/network/well_known_names.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Kafka {

namespace {

/**
 * All stats for Kafka simple proxy. @see stats_macros.h
 */
// clang-format off
#define ALL_KAFKA_SIMPLE_STATS(COUNTER) \
  COUNTER(decoder_error)
// clang-format on

/**
 * Struct definition for all stats for Kafka simple proxy. @see stats_macros.h
 */
struct KafkaSimpleStats {
  ALL_KAFKA_SIMPLE_STATS(GENERATE_COUNTER_STRUCT)
};

struct KafkaSimpleConfig {
  KafkaSimpleConfig(std::string broker_host, std::uint32_t broker_port,
                    const std::string& stat_prefix, Stats::Scope& scope)
      : broker_host(std::move(broker_host)), broker_port(broker_port), scope(scope),
        stats(generateStats(stat_prefix, scope)), stat_name_set(scope.symbolTable()),
        stat_prefix(stat_name_set.add(stat_prefix)),
        connect_latency(stat_name_set.add("connect_latency")) {}

  const std::string broker_host;
  const std::uint16_t broker_port;
  Stats::Scope& scope;
  KafkaSimpleStats stats;
  Stats::StatNameSet stat_name_set;
  const Stats::StatName stat_prefix;
  const Stats::StatName connect_latency;

private:
  KafkaSimpleStats generateStats(const std::string& prefix, Stats::Scope& scope) {
    return KafkaSimpleStats{ALL_KAFKA_SIMPLE_STATS(POOL_COUNTER_PREFIX(scope, prefix))};
  }
};

class SimpleKafkaFilter : public Network::Filter, Logger::Loggable<Logger::Id::kafka> {
public:
  SimpleKafkaFilter(std::shared_ptr<KafkaSimpleConfig> config,
                    Server::Configuration::FactoryContext& context)
      : config_(std::move(config)), context_(context) {}

  Network::FilterStatus onWrite(Buffer::Instance& /* data */, bool /* end_stream */) override {
    return Network::FilterStatus::Continue;
  }

  Network::FilterStatus onData(Buffer::Instance& /* data */, bool /* end_stream */) override {
    return Network::FilterStatus::Continue;
  }

  Network::FilterStatus onNewConnection() override { return Network::FilterStatus::Continue; }

  void initializeReadFilterCallbacks(Network::ReadFilterCallbacks& callbacks) override {
    read_callbacks_ = &callbacks;
    ENVOY_LOG(info, "LocalInfo.Address: {}", context_.localInfo().address()->asString());
  }

private:
  std::shared_ptr<KafkaSimpleConfig> config_;
  Server::Configuration::FactoryContext& context_;
  Network::ReadFilterCallbacks* read_callbacks_ = nullptr;
};
} // namespace

class KafkaSimpleConfigFactory
    : public Common::FactoryBase<envoy::config::filter::network::kafka::v1alpha1::KafkaSimple> {
public:
  KafkaSimpleConfigFactory() : FactoryBase(NetworkFilterNames::get().KafkaSimple) {}

private:
  Network::FilterFactoryCb createFilterFactoryFromProtoTyped(
      const envoy::config::filter::network::kafka::v1alpha1::KafkaSimple& proto_config,
      Server::Configuration::FactoryContext& context) override {
    std::string stat_prefix = fmt::format("{}.kafka_simple.", proto_config.stat_prefix());

    auto config = std::make_shared<KafkaSimpleConfig>(
        proto_config.broker_host(), proto_config.broker_port(), stat_prefix, context.scope());

    return [config, &context](Network::FilterManager& filter_manager) {
      filter_manager.addFilter(std::make_shared<SimpleKafkaFilter>(config, context));
    };
  }
};

/**
 * Static registration for the Kafka simple filter. @see RegisterFactory.
 */
REGISTER_FACTORY(KafkaSimpleConfigFactory, Server::Configuration::NamedNetworkFilterConfigFactory);

} // namespace Kafka
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
