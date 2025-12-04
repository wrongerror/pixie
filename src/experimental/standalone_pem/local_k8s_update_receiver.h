/*
 * Copyright 2018- The Pixie Authors.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>

#include "src/common/base/base.h"
#include "src/shared/k8s/metadatapb/metadata.pb.h"
#include "src/shared/metadata/state_manager.h"

namespace px {
namespace vizier {
namespace agent {

// A minimal local receiver that forwards ResourceUpdate to the AgentMetadataStateManagerImpl.
// This is a placeholder to be backed by a transport (UDS/gRPC) in a follow-up change.
class LocalK8sUpdateReceiver {
 public:
  explicit LocalK8sUpdateReceiver(px::md::AgentMetadataStateManager* mds_manager)
      : mds_manager_(mds_manager) {}

  Status Push(const px::shared::k8s::metadatapb::ResourceUpdate& update) {
    return mds_manager_->AddK8sUpdate(std::make_unique<px::shared::k8s::metadatapb::ResourceUpdate>(update));
  }

 private:
  px::md::AgentMetadataStateManager* mds_manager_;
};

}  // namespace agent
}  // namespace vizier
}  // namespace px
