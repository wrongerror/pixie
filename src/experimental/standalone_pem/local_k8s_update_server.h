/*
 * Copyright 2018- The Pixie Authors.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <string>
#include <thread>

#include "src/common/base/base.h"
#include "src/experimental/standalone_pem/local_k8s_update_receiver.h"

namespace px {
namespace vizier {
namespace agent {

// A simple UNIX domain socket server that receives length-prefixed ResourceUpdate
// protobuf messages and forwards them to LocalK8sUpdateReceiver.
class LocalK8sUpdateServer {
 public:
  explicit LocalK8sUpdateServer(LocalK8sUpdateReceiver* receiver)
      : receiver_(receiver) {}

  Status Start();
  void Stop();

 private:
  Status ServerLoop();
  std::string SocketPath();

  LocalK8sUpdateReceiver* receiver_;
  std::thread server_thread_;
  std::atomic<bool> stop_{false};
};

}  // namespace agent
}  // namespace vizier
}  // namespace px
