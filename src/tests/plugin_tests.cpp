// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <tuple>
#include <vector>

#include <mesos/mesos.hpp>

#include <google/protobuf/util/json_util.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>

#include <process/gmock.hpp>
#include <process/grpc.hpp>
#include <process/owned.hpp>
#include <process/gtest.hpp>

#include <stout/gtest.hpp>

#include <mesos/plugins/isolator/isolator.pb.h>
#include <mesos/plugins/isolator/isolator.grpc.pb.h>

#include "slave/containerizer/mesos/containerizer.hpp"

#include "slave/paths.hpp"

#include "tests/cluster.hpp"
#include "tests/environment.hpp"
#include "tests/mesos.hpp"
#include "tests/containerizer/docker_archive.hpp"

using std::string;
using std::vector;
using std::unique_ptr;

using process::Future;
using process::Owned;
using process::grpc::client::Connection;

using ::grpc::ChannelArguments;
using ::grpc::InsecureServerCredentials;
using ::grpc::Server;
using ::grpc::ServerBuilder;
using ::grpc::ServerContext;

using mesos::internal::master::Master;

using mesos::internal::slave::Containerizer;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::MesosContainerizer;
using mesos::internal::slave::Slave;

using mesos::master::detector::MasterDetector;

using mesos::slave::ContainerTermination;

using namespace mesos::plugins::isolator;

namespace mesos {
namespace internal {
namespace tests {

// Default imprementation for output protobuf messages (copied from csi).
// Note that any non-template overloading of the output operator
// would take precedence over this function template.
template <
  typename Message,
  typename std::enable_if<
    std::is_convertible<Message*, google::protobuf::Message*>::value,
    int>::type = 0>
std::ostream& operator<<(std::ostream& stream, const Message& message)
{
  // NOTE: We use Google's JSON utility functions for proto3.
  std::string output;
  google::protobuf::util::MessageToJsonString(message, &output);
  return stream << output;
}


class IsolatorPluginService : public IsolatorPlugin::Service
{
public:
  virtual ~IsolatorPluginService() {}

  ::grpc::Status Initialize(
    ServerContext* context,
    const InitializeRequest* request,
    InitializeResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Initialize called with " << *request;
    response->set_support_nested(false);
    return ::grpc::Status::OK;
  }

  ::grpc::Status Recover(
    ServerContext* context,
    const RecoverRequest* request,
    RecoverResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Recover called with " << *request;
    return ::grpc::Status::OK;
  }

  ::grpc::Status Prepare(
    ServerContext* context,
    const PrepareRequest* request,
    PrepareResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Prepare called with " << *request;
    return ::grpc::Status::OK;
  }

  ::grpc::Status Isolate(
    ServerContext* context,
    const IsolateRequest* request,
    IsolateResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Isolate called with " << *request;
    return ::grpc::Status::OK;
  }

  ::grpc::Status Update(
    ServerContext* context,
    const UpdateRequest* request,
    UpdateResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Update called with " << *request;
    return ::grpc::Status::OK;
  }


  ::grpc::Status Watch(
    ServerContext* context,
    const WatchRequest* request,
    WatchResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Watch called with " << *request;
    return ::grpc::Status::OK;
  }

  ::grpc::Status Usage(
    ServerContext* context,
    const UsageRequest* request,
    UsageResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Usage called with " << *request;
    return ::grpc::Status::OK;
  }

  ::grpc::Status Status(
    ServerContext* context,
    const StatusRequest* request,
    StatusResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Status called with " << *request;
    return ::grpc::Status::OK;
  }

  ::grpc::Status Cleanup(
    ServerContext* context,
    const CleanupRequest* request,
    CleanupResponse* response) override
  {
    LOG(INFO) << "IsolatorPlugin::Cleanup called with " << *request;
    return ::grpc::Status::OK;
  }
};


class PluginTest : public MesosTest {
public:
  string server_address() const
  {
    return "unix://" + path::join(sandbox.get(), "socket");
  }

  virtual void SetUp()
  {
    MesosTest::SetUp();

    service.reset(new IsolatorPluginService());
    ServerBuilder builder;
    builder.AddListeningPort(server_address(), InsecureServerCredentials());
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
    CHECK(server) << "Unable to start a gRPC server";

    connection.reset(new Connection(server_address()));
  }

  virtual void TearDown()
  {
    if (server) {
      server->Shutdown();
      server->Wait();
      server.reset();
    }

    MesosTest::TearDown();
  }

  unique_ptr<IsolatorPluginService> service;
  unique_ptr<Server> server;
  unique_ptr<Connection> connection;
};


// This test verifies that the root filesystem of the container is
// properly changed to the one that's provisioned by the provisioner.
TEST_F(PluginTest, ROOT_InitialIntegration)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  string registry = path::join(sandbox.get(), "registry");
  AWAIT_READY(DockerArchive::create(registry, "test_image"));

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "filesystem/linux,docker/runtime,plugins/external";
  flags.external_isolator_sockets = server_address();
  flags.docker_registry = registry;
  flags.docker_store_dir = path::join(sandbox.get(), "store");
  flags.image_providers = "docker";

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;

  MesosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      master.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  const Offer& offer = offers.get()[0];

  TaskInfo task = createTask(
      offer.slave_id(),
      offer.resources(),
      "test -d " + flags.sandbox_directory);

  task.mutable_container()->CopyFrom(createContainerInfo("test_image"));

  driver.launchTasks(offer.id(), {task});

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  AWAIT_READY(statusStarting);
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  driver.stop();
  driver.join();
}


} // namespace tests {
} // namespace internal {
} // namespace mesos {
