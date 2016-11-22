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

#include <set>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>

#include <mesos/quota/quota.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include "master/flags.hpp"
#include "master/master.hpp"

#include "slave/slave.hpp"

#include "tests/allocator.hpp"
#include "tests/containerizer.hpp"
#include "tests/mesos.hpp"

using google::protobuf::RepeatedPtrField;

using mesos::internal::master::Master;

using mesos::internal::slave::Slave;

using mesos::master::detector::MasterDetector;

using mesos::quota::QuotaRequest;
using mesos::quota::QuotaStatus;

using process::Clock;
using process::Future;
using process::Owned;
using process::PID;

using process::http::Accepted;
using process::http::BadRequest;
using process::http::Conflict;
using process::http::Forbidden;
using process::http::OK;
using process::http::Response;
using process::http::Unauthorized;

using std::string;
using std::vector;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Eq;

namespace mesos {
namespace internal {
namespace tests {

// Quota tests that are allocator-agnostic (i.e. we expect every
// allocator to implement basic quota guarantees) are in this
// file. All tests are split into logical groups:
//   * Request validation tests.
//   * Sanity check tests.
//   * Quota functionality tests.
//   * Failover and recovery tests.
//   * Authentication and authorization tests.

// TODO(alexr): Once we have other allocators, convert this test into a
// typed test over multiple allocators.
class MasterQuotaTest : public MesosTest
{
protected:
  MasterQuotaTest()
  {
    // We reuse default agent resources and expect them to be sufficient.
    defaultAgentResources = Resources::parse(defaultAgentResourcesString).get();
    CHECK(defaultAgentResources.contains(Resources::parse(
          "cpus:2;gpus:0;mem:1024;disk:1024;ports:[31000-32000]").get()));
  }

  // Sets up the master flags with three roles and a short allocation interval.
  virtual master::Flags CreateMasterFlags()
  {
    master::Flags flags = MesosTest::CreateMasterFlags();
    flags.allocation_interval = Milliseconds(50);
    flags.roles = strings::join(",", ROLE1, ROLE2, ROLE3);
    return flags;
  }

  // Creates a FrameworkInfo with the specified role.
  FrameworkInfo createFrameworkInfo(const string& role)
  {
    FrameworkInfo info;
    info.set_user("user");
    info.set_name("framework" + process::ID::generate());
    info.mutable_id()->set_value(info.name());
    info.set_role(role);

    return info;
  }

  // Generates a quota HTTP request body from the specified resources and role.
  string createRequestBody(
      const string& role,
      const Resources& resources,
      bool force = false) const
  {
    QuotaRequest request;

    request.set_role(role);
    request.mutable_guarantee()->CopyFrom(
        static_cast<const RepeatedPtrField<Resource>&>(resources));

    if (force) {
      request.set_force(force);
    }

    return stringify(JSON::protobuf(request));
  }

protected:
  const string ROLE1{"role1"};
  const string ROLE2{"role2"};
  const string ROLE3{"role3"};
  const string UNKNOWN_ROLE{"unknown"};

  Resources defaultAgentResources;
};


// These are request validation tests. They verify JSON is well-formed,
// convertible to corresponding protobufs, all necessary fields are present,
// while irrelevant fields are not present.

// Verifies that a request for a non-existent role is rejected when
// using an explicitly configured list of role names.
TEST_F(MasterQuotaTest, SetForNonExistentRole)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // We do not need an agent since a request should be rejected before we
  // start looking at available resources.

  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

  // Send a quota request for a non-existent role.
  Future<Response> response = process::http::post(
      master.get()->pid,
      "quota",
      createBasicAuthHeaders(DEFAULT_CREDENTIAL),
      createRequestBody("non-existent-role", quotaResources));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
    << response.get().body;
}


// Verifies that various set requests for incorrect role usage are
// rejected.
TEST_F(MasterQuotaTest, SetIncorrectRole)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // We do not need an agent since a request should be rejected before we
  // start looking at available resources.

  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

  // Send a quota request without role.
  {
    QuotaRequest request;
    request.mutable_guarantee()->CopyFrom(
        static_cast<const RepeatedPtrField<Resource>&>(quotaResources));

    string requestBody = stringify(JSON::protobuf(request));

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        requestBody);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Send a quota request for wildcard role.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody("*", quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Send a quota request with empty string role name.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody("", quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }
}


// Quota requests with invalid structure should return a '400 Bad Request'.
TEST_F(MasterQuotaTest, InvalidSetRequest)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // We do not need an agent since a request should be rejected before
  // we start looking at available resources.

  // Wrap the `http::post` into a lambda for readability of the test.
  auto postQuota = [this, &master](const string& request) {
    return process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        request);
  };

  // Tests whether a quota request with invalid JSON fails.
  {
    const string badRequest =
      "{"
      "  invalidJson"
      "}";

    Future<Response> response = postQuota(badRequest);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Tests whether a quota request with missing 'role' field fails.
  {
    const string badRequest =
      "{"
      "  \"resources\":["
      "    {"
      "      \"name\":\"cpus\","
      "      \"type\":\"SCALAR\","
      "      \"scalar\":{\"value\":1}"
      "    }"
      "  ]"
      "}";

    Future<Response> response = postQuota(badRequest);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Tests whether a quota request with missing 'resource' field fails.
  {
    const string badRequest =
      "{"
      "  \"role\":\"some-role\","
      "  \"unknownField\":\"unknownValue\""
      "}";

    Future<Response> response = postQuota(badRequest);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Tests whether a quota request with invalid resources fails.
  {
    const string badRequest =
      "{"
      "  \"resources\":"
      "  ["
      "    \" invalidResource\" : 1"
      "  ]"
      "}";

    Future<Response> response = postQuota(badRequest);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }
}


// Checks that a quota set request is not satisfied if any invalid field is
// set or provided data are not supported.
TEST_F(MasterQuotaTest, SetRequestWithInvalidData)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // We do not need an agent since a request should be rejected before
  // we start looking at available resources.

  // Wrap the `http::post` into a lambda for readability of the test.
  auto postQuota = [this, &master](
      const string& role, const Resources& resources) {
    return process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(role, resources));
  };

  // A quota set request with non-scalar resources (e.g. ports) should return
  // '400 Bad Request'.
  {
    Resources quotaResources =
      Resources::parse("cpus:1;mem:512;ports:[31000-31001]").get();

    Future<Response> response = postQuota(ROLE1, quotaResources);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // A quota set request with a role set in any of the `Resource` objects
  // should return '400 Bad Request'.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512", ROLE1).get();

    Future<Response> response = postQuota(ROLE1, quotaResources);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // A quota set request with a resource name repeated more than once
  // should return '400 Bad Request'.
  {
    Resources quotaResources =
      Resources::parse("cpus:1;mem:512;cpus:1", ROLE1).get();

    Future<Response> response = postQuota(ROLE1, quotaResources);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }


  // A quota set request with the `DiskInfo` field set should return
  // '400 Bad Request'.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Resource volume = Resources::parse("disk", "128", ROLE1).get();
    volume.mutable_disk()->CopyFrom(createDiskInfo("id1", "path1"));
    quotaResources += volume;

    Future<Response> response = postQuota(ROLE1, quotaResources);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // A quota set request with the `RevocableInfo` field set should return
  // '400 Bad Request'.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Resource revocable = Resources::parse("cpus", "1", "*").get();
    revocable.mutable_revocable();
    quotaResources += revocable;

    Future<Response> response = postQuota(ROLE1, quotaResources);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // A quota set request with the `ReservationInfo` field set should return
  // '400 Bad Request'.
  {
    Resources quotaResources = Resources::parse("cpus:4;mem:512").get();

    Resource reserved = Resources::parse("disk", "128", ROLE1).get();
    reserved.mutable_reservation()->CopyFrom(
        createReservationInfo(DEFAULT_CREDENTIAL.principal()));

    quotaResources += reserved;

    Future<Response> response = postQuota(ROLE1, quotaResources);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }
}


// Updating an exiting quota on the '/master/quota endpoint'.
TEST_F(MasterQuotaTest, UpdateExistingQuota)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

  // Update quota for the role without quota set via put should error.
  {
    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, FORCE)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Set quota for the role without quota set.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  quotaResources = Resources::parse("cpus:2;mem:256").get();
  // Try to set quota via post should error out.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Update quota for the role via put should pass.
  {
    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, FORCE)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Query the master quota endpoint to make sure the quota returned
  // is new value.
  {
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    ASSERT_EQ(1, status.get().infos().size());
    EXPECT_EQ(quotaResources, status.get().infos(0).guarantee());
  }
}


// Tests whether we can remove a quota from the '/master/quota endpoint' via a
// DELETE request against /quota.
TEST_F(MasterQuotaTest, RemoveSingleQuota)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  // Wrap the `http::requestDelete` into a lambda for readability of the test.
  auto removeQuota = [this, &master](const string& path) {
    return process::http::requestDelete(
        master.get()->pid,
        path,
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));
  };

  // Ensure that we can't remove quota for a role that is unknown to
  // the master when using explicitly configured list of role names.
  {
    Future<Response> response = removeQuota("quota/" + UNKNOWN_ROLE);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Ensure that we can't remove quota for a role that has no quota set.
  {
    Future<Response> response = removeQuota("quota/" + ROLE1);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response)
      << response.get().body;
  }

  // Ensure we can remove the quota we have requested before.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    // Remove the previously requested quota.
    Future<Nothing> receivedRemoveRequest;
    EXPECT_CALL(allocator, removeQuota(Eq(ROLE1)))
      .WillOnce(DoAll(InvokeRemoveQuota(&allocator),
                      FutureSatisfy(&receivedRemoveRequest)));

    response = removeQuota("quota/" + ROLE1);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    // Ensure that the quota remove request has reached the allocator.
    AWAIT_READY(receivedRemoveRequest);
  }
}


// Tests whether we can retrieve the current quota status from
// /master/quota endpoint via a GET request against /quota.
TEST_F(MasterQuotaTest, Status)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

  // Query the master quota endpoint when no quota is set.
  {
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    EXPECT_EQ(0, status.get().infos().size());
  }

  // Send a quota request for the specified role.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Query the master quota endpoint when quota is set for a single role.
  {
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    ASSERT_EQ(1, status.get().infos().size());
    EXPECT_EQ(quotaResources, status.get().infos(0).guarantee());
  }
}


// These tests check whether a request makes sense in terms of current cluster
// status. A quota request may be well-formed, but obviously infeasible, e.g.
// request for 100 CPUs in a cluster with just 11 CPUs.

// Checks that a quota request is not satisfied if there are not enough
// resources on a single agent and the force flag is not set.
TEST_F(MasterQuotaTest, InsufficientResourcesSingleAgent)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start an agent and wait until its resources are available.
  Future<Resources> agentTotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agentTotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get());
  ASSERT_SOME(slave);

  AWAIT_READY(agentTotalResources);
  EXPECT_EQ(defaultAgentResources, agentTotalResources.get());

  const Resources clusterResources =
    agentTotalResources.get().filter(
        [=](const Resource& resource) {
          return (resource.name() == "cpus" || resource.name() == "mem");
        });

  // Our quota request requires more resources than available on the agent
  // (and in the cluster).
  Resources quotaResources =
    clusterResources + Resources::parse("cpus:1;mem:1024").get();

  EXPECT_FALSE(agentTotalResources.get().contains(quotaResources));

  // Since there are not enough resources in the cluster, `capacityHeuristic`
  // check fails rendering the request unsuccessful.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Conflict().status, response)
      << response.get().body;
  }

  // Force flag should override the `capacityHeuristic` check and make the
  // request succeed.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, true));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Update the quota to somewhere below current quota value but still
  // above cluster capacity. We skip `capacityHeuristic` check in this
  // case, so the request should succeed.
  quotaResources = clusterResources + Resources::parse("mem:1024").get();

  {
    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, false)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Update the quota to within cluster capacity should be allowed.
  quotaResources =
    agentTotalResources.get().filter(
        [=](const Resource& resource) {
          return (resource.name() == "cpus" || resource.name() == "mem");
        });
  {
    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, true)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that a quota request is not satisfied if there are not enough
// resources on multiple agents and the force flag is not set.
TEST_F(MasterQuotaTest, InsufficientResourcesMultipleAgents)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Start another agent and wait until its resources are available.
  Future<Resources> agent2TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent2TotalResources)));

  Try<Owned<cluster::Slave>> slave2 = StartSlave(detector.get());
  ASSERT_SOME(slave2);

  AWAIT_READY(agent2TotalResources);
  EXPECT_EQ(defaultAgentResources, agent2TotalResources.get());

  // Our quota request requires more resources than available on the agent
  // (and in the cluster).
  Resources quotaResources =
    agent1TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    }) +
    agent2TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    }) +
    Resources::parse("cpus:1;mem:1024").get();

  EXPECT_FALSE((agent1TotalResources.get() + agent2TotalResources.get())
    .contains(quotaResources));

  // Since there are not enough resources in the cluster, `capacityHeuristic`
  // check fails which rendering the request unsuccessful.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Conflict().status, response)
      << response.get().body;
  }

  // Force flag should override the `capacityHeuristic` check and make the
  // request succeed.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, true));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that an operator can request quota when enough resources are
// available on single agent.
TEST_F(MasterQuotaTest, AvailableResourcesSingleAgent)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start an agent and wait until its resources are available.
  Future<Resources> agentTotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agentTotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get());
  ASSERT_SOME(slave);

  AWAIT_READY(agentTotalResources);
  EXPECT_EQ(defaultAgentResources, agentTotalResources.get());

  // We request quota for a portion of resources available on the agent.
  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();
  EXPECT_TRUE(agentTotalResources.get().contains(quotaResources));

  // Send a quota request for the specified role.
  Future<Quota> receivedQuotaRequest;
  EXPECT_CALL(allocator, setQuota(Eq(ROLE1), _))
    .WillOnce(DoAll(InvokeSetQuota(&allocator),
                    FutureArg<1>(&receivedQuotaRequest)));

  Future<Response> response = process::http::post(
      master.get()->pid,
      "quota",
      createBasicAuthHeaders(DEFAULT_CREDENTIAL),
      createRequestBody(ROLE1, quotaResources));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response) << response.get().body;

  // Quota request is granted and reached the allocator. Make sure nothing
  // got lost in-between.
  AWAIT_READY(receivedQuotaRequest);

  EXPECT_EQ(ROLE1, receivedQuotaRequest.get().info.role());
  EXPECT_EQ(quotaResources, receivedQuotaRequest.get().info.guarantee());
}


// Checks that an operator can request quota when enough resources are
// available in the cluster, but not on a single agent.
TEST_F(MasterQuotaTest, AvailableResourcesMultipleAgents)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Start another agent and wait until its resources are available.
  Future<Resources> agent2TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent2TotalResources)));

  Try<Owned<cluster::Slave>> slave2 = StartSlave(detector.get());
  ASSERT_SOME(slave2);

  AWAIT_READY(agent2TotalResources);
  EXPECT_EQ(defaultAgentResources, agent2TotalResources.get());

  // We request quota for a portion of resources, which is not available
  // on a single agent.
  Resources quotaResources =
    agent1TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    }) +
    agent2TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    });

  // Send a quota request for the specified role.
  Future<Quota> receivedQuotaRequest;
  EXPECT_CALL(allocator, setQuota(Eq(ROLE1), _))
    .WillOnce(DoAll(InvokeSetQuota(&allocator),
                    FutureArg<1>(&receivedQuotaRequest)));

  Future<Response> response = process::http::post(
      master.get()->pid,
      "quota",
      createBasicAuthHeaders(DEFAULT_CREDENTIAL),
      createRequestBody(ROLE1, quotaResources));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response) << response.get().body;

  // Quota request is granted and reached the allocator. Make sure nothing
  // got lost in-between.
  AWAIT_READY(receivedQuotaRequest);

  EXPECT_EQ(ROLE1, receivedQuotaRequest.get().info.role());
  EXPECT_EQ(quotaResources, receivedQuotaRequest.get().info.guarantee());
}


// Checks that a quota request succeeds if there are sufficient total
// resources in the cluster, even though they are blocked in outstanding
// offers, i.e. quota request rescinds offers.
TEST_F(MasterQuotaTest, AvailableResourcesAfterRescinding)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Start another agent and wait until its resources are available.
  Future<Resources> agent2TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent2TotalResources)));

  Try<Owned<cluster::Slave>> slave2 = StartSlave(detector.get());
  ASSERT_SOME(slave2);

  AWAIT_READY(agent2TotalResources);
  EXPECT_EQ(defaultAgentResources, agent2TotalResources.get());

  // Start one more agent and wait until its resources are available.
  Future<Resources> agent3TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent3TotalResources)));

  Try<Owned<cluster::Slave>> slave3 = StartSlave(detector.get());
  ASSERT_SOME(slave3);

  AWAIT_READY(agent3TotalResources);
  EXPECT_EQ(defaultAgentResources, agent3TotalResources.get());

  // We start with the following cluster setup.
  // Total cluster resources (3 identical agents): cpus=6, mem=3072.
  // role1 share = 0
  // role2 share = 0

  // We create a "hoarding" framework that will hog the resources but
  // will not use them.
  FrameworkInfo frameworkInfo1 = createFrameworkInfo(ROLE1);
  MockScheduler sched1;
  MesosSchedulerDriver framework1(
      &sched1, frameworkInfo1, master.get()->pid, DEFAULT_CREDENTIAL);

  // We use `offers` to capture offers from the `resourceOffers()` callback.
  Future<vector<Offer>> offers;

  // Set expectations for the first offer and launch the framework.
  // In this test we care about rescinded resources and not about
  // following allocations, hence ignore any subsequent offers.
  EXPECT_CALL(sched1, registered(&framework1, _, _));
  EXPECT_CALL(sched1, resourceOffers(&framework1, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  framework1.start();

  // In the first offer, expect offers from all available agents.
  AWAIT_READY(offers);
  ASSERT_EQ(3u, offers.get().size());

  // `framework1` hoards the resources, i.e. does not accept them.
  // Now we add two new frameworks to `ROLE2`, for which we should
  // make space if we can.

  FrameworkInfo frameworkInfo2 = createFrameworkInfo(ROLE2);
  MockScheduler sched2;
  MesosSchedulerDriver framework2(
      &sched2, frameworkInfo2, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered2;
  EXPECT_CALL(sched2, registered(&framework2, _, _))
    .WillOnce(FutureSatisfy(&registered2));

  FrameworkInfo frameworkInfo3 = createFrameworkInfo(ROLE2);
  MockScheduler sched3;
  MesosSchedulerDriver framework3(
      &sched3, frameworkInfo3, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered3;
  EXPECT_CALL(sched3, registered(&framework3, _, _))
    .WillOnce(FutureSatisfy(&registered3));

  framework2.start();
  framework3.start();

  AWAIT_READY(registered2);
  AWAIT_READY(registered3);

  // There should be no offers made to `framework2` and `framework3`
  // after they are started, since there are no free resources. They
  // may receive offers once we set the quota.

  // Total cluster resources (3 identical agents): cpus=6, mem=3072.
  // role1 share = 1 (cpus=6, mem=3072)
  //   framework1 share = 1
  // role2 share = 0
  //   framework2 share = 0
  //   framework3 share = 0

  // We request quota for a portion of resources which is smaller than
  // the total cluster capacity and can fit into any single agent.
  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

  // Once the quota request reaches the master, it should trigger a series
  // of rescinds. Even though quota request resources can be satisfied with
  // resources from a single agent, offers from two agents must be rescinded,
  // because there are two frameworks in the quota'ed role `ROLE2`.
  Future<Nothing> offerRescinded1, offerRescinded2;
  EXPECT_CALL(sched1, offerRescinded(&framework1, _))
    .WillOnce(FutureSatisfy(&offerRescinded1))
    .WillOnce(FutureSatisfy(&offerRescinded2));

  // In this test we are not interested in which frameworks will be offered
  // the rescinded resources.
  EXPECT_CALL(sched2, resourceOffers(&framework2, _))
    .WillRepeatedly(Return());

  EXPECT_CALL(sched3, resourceOffers(&framework3, _))
    .WillRepeatedly(Return());

  // Send a quota request for the specified role.
  Future<Quota> receivedQuotaRequest;
  EXPECT_CALL(allocator, setQuota(Eq(ROLE2), _))
    .WillOnce(DoAll(InvokeSetQuota(&allocator),
                    FutureArg<1>(&receivedQuotaRequest)));

  Future<Response> response = process::http::post(
      master.get()->pid,
      "quota",
      createBasicAuthHeaders(DEFAULT_CREDENTIAL),
      createRequestBody(ROLE2, quotaResources));

  // At some point before the response is sent, offers are rescinded,
  // but resources are not yet allocated. At this moment the cluster
  // state looks like this.

  // Total cluster resources (3 identical agents): cpus=6, mem=3072.
  // role1 share = 0.33 (cpus=2, mem=1024)
  //   framework1 share = 1
  // role2 share = 0
  //   framework2 share = 0
  //   framework3 share = 0

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response) << response.get().body;

  // The quota request is granted and reached the allocator. Make sure nothing
  // got lost in-between.
  AWAIT_READY(receivedQuotaRequest);
  EXPECT_EQ(ROLE2, receivedQuotaRequest.get().info.role());
  EXPECT_EQ(quotaResources, receivedQuotaRequest.get().info.guarantee());

  // Ensure `RescindResourceOfferMessage`s are processed by `sched1`.
  AWAIT_READY(offerRescinded1);
  AWAIT_READY(offerRescinded2);

  framework1.stop();
  framework1.join();

  framework2.stop();
  framework2.join();

  framework3.stop();
  framework3.join();
}


// Checks that a quota request is still satisfied even
// if tasks from other roles use up the capacity, because
// we do not count tasks on unreserved resources when
// performing capacity check.
TEST_F(MasterQuotaTest, AvailableResourcesWithTasks)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));


  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);
  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave1 =
    StartSlave(detector.get(), &containerizer);

  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Start a scheduler and a task.
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role(ROLE1);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched,
      frameworkInfo,
      master.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(_, _, _));

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());


  TaskInfo task = createTask(offers.get()[0], "", DEFAULT_EXECUTOR_ID);

  EXPECT_CALL(exec, registered(_, _, _, _));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> status;

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_RUNNING, status.get().state());

  Resources clusterResources =
    defaultAgentResources.get("cpus") +
    defaultAgentResources.get("mems");

  // Submit a quota request same to current agent resources for role2,
  // This should still be satisified because resource usage of the task
  // is not included in the heuristic check.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE2, clusterResources, false));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// Checks that a quota request is satisfied even if there are enough
// resources on multiple agents, but some part is dynamically reserved
// for other roles.
TEST_F(MasterQuotaTest, InsufficientResourcesWithDynamicReservation)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<SlaveID> slaveId;
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId),
                    FutureArg<3>(&agent1TotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(slaveId);
  AWAIT_READY(agent1TotalResources);

  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Start another agent and wait until its resources are available.
  Future<Resources> agent2TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent2TotalResources)));

  Try<Owned<cluster::Slave>> slave2 = StartSlave(detector.get());
  ASSERT_SOME(slave2);

  AWAIT_READY(agent2TotalResources);
  EXPECT_EQ(defaultAgentResources, agent2TotalResources.get());

  // Send a dynamic reserve operation for all cpus on agent1 to role1.
  Resources toReserve =
    agent1TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus");
  });

  Resources dynamicallyReserved = toReserve.flatten(
      ROLE1,
      createReservationInfo(DEFAULT_CREDENTIAL.principal())).get();

  const google::protobuf::RepeatedPtrField<Resource>& repeated =
    dynamicallyReserved;

  string reservationBody = strings::format(
      "slaveId=%s&resources=%s",
      slaveId->value(),
      JSON::protobuf(repeated)).get();

  Future<CheckpointResourcesMessage> checkpointResources =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, slave1.get()->pid);

  Future<Response> reservationResponse = process::http::post(
      master.get()->pid,
      "reserve",
      createBasicAuthHeaders(DEFAULT_CREDENTIAL),
      reservationBody);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(Accepted().status, reservationResponse)
    << reservationResponse.get().body;

  // If checkpoint resources is received on slave1, then master
  // have applied the reservation operation internally.
  AWAIT_READY(checkpointResources);

  // Our quota request requires total amount of cpus and mem in the cluster.
  Resources quotaResources =
    agent1TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    }) +
    agent2TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    });

  EXPECT_TRUE((agent1TotalResources.get() + agent2TotalResources.get())
    .contains(quotaResources));

  // Dynamically reserved resources are still counted as available
  // towards quota for now, so requesting can still be satisfied.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE2, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that a quota request is not satisfied if there are enough
// resources on multiple agents, but some part is statically reserved.
TEST_F(MasterQuotaTest, InsufficientResourcesWithStaticReservation)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Start another agent whose cpus resource is statically reserved,
  // and wait until its resources are available.
  string slave2Resources =
    "cpus(role1):1;mem:512;disk:1024;ports:[31000-32000]";
  slave::Flags slave2Flags = CreateSlaveFlags();
  slave2Flags.resources = slave2Resources;
  Future<Resources> agent2TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent2TotalResources)));

  Try<Owned<cluster::Slave>> slave2 =
    StartSlave(detector.get(), slave2Flags);
  ASSERT_SOME(slave2);

  AWAIT_READY(agent2TotalResources);
  EXPECT_EQ(Resources::parse(slave2Resources).get(),
            agent2TotalResources.get());

  // Our quota request requires more resources than available on the agent
  // (and in the cluster).
  Resources quotaResources =
    agent1TotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    }) +
    Resources::parse("cpus:1;mem:512").get();

  // Because statically reserved resources are not counted in heuristic
  // check, quota request should fail.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Conflict().status, response)
      << response.get().body;
  }

  // Force flag should override the `capacityHeuristic` check and make the
  // request succeed.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, true));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that a quota request can be created when there is still
// enough resources after multiple existing quotas.
TEST_F(MasterQuotaTest, SufficientResourcesMultipleQuotas)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));


  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  Resources role1Quota = Resources::parse("cpus:1").get();
  Resources role2Quota = Resources::parse("mem:512").get();
  Resources role3Quota = Resources::parse("cpus:1;mem:512").get();

  EXPECT_TRUE(defaultAgentResources.contains(
      role1Quota + role2Quota + role3Quota));

  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, role1Quota));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE2, role2Quota));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE3, role3Quota));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that disconnected slave is not counted in `capacityHeuristic`.
TEST_F(MasterQuotaTest, InsufficientResourceAfterSlaveDisconnect)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));


  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Future<process::Message> slaveRegisteredMessage =
    FUTURE_MESSAGE(Eq(SlaveRegisteredMessage().GetTypeName()), _, _);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(slaveRegisteredMessage);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  Resources quotaResources =
    defaultAgentResources.filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    });

  // Make sure deactivateSlave is called on allocator.
  Future<SlaveID> deactivatedSlaveID;
  EXPECT_CALL(allocator, deactivateSlave(_))
    .WillOnce(DoAll(InvokeDeactivateSlave(&allocator),
                    FutureArg<0>(&deactivatedSlaveID)));

  // Inject a slave exited event at the master causing the master
  // to mark the slave as disconnected.
  process::inject::exited(slaveRegisteredMessage.get().to, master.get()->pid);

  // Wait until allocator deactivates the slave.
  AWAIT_READY(deactivatedSlaveID);

  // There is no resource left after master deactivates the slave,
  // so request should fail due to `capacityHeuristic` check.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Conflict().status, response)
      << response.get().body;
  }

  // Force flag should override the `capacityHeuristic` check and make the
  // request succeed.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, true));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that a quota request cannot be satisfied without
// enough resources after multiple existing quotas.
TEST_F(MasterQuotaTest, InsufficientResourcesMultipleQuotas)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));


  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  Resources role1Quota = Resources::parse("cpus:1").get();
  Resources role2Quota = Resources::parse("mem:512").get();
  Resources role3Quota =
    defaultAgentResources.filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    });

  EXPECT_TRUE(defaultAgentResources.contains(role1Quota + role2Quota));
  EXPECT_FALSE(
    defaultAgentResources.contains(role1Quota + role2Quota + role3Quota));

  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, role1Quota));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE2, role2Quota));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // There is not enough resource for role3 quota request,
  // so request should fail due to `capacityHeuristic` check.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE3, role3Quota));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Conflict().status, response)
      << response.get().body;
  }

  // Force flag should override the `capacityHeuristic` check and make the
  // request succeed.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE3, role3Quota, true));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// These tests ensure quota implements declared functionality. Note that the
// tests here are allocator-agnostic, which means we expect every allocator to
// implement basic quota guarantees.

// TODO(alexr): Tests to implement:
//   * Role quota is below its allocation (InverseOffer generation).


// Checks that an agent with quota'ed tasks disconnects and there are not enough
// free resources (alert and under quota situation).
TEST_F(MasterQuotaTest, InsufficientAfterSlaveWithQuotaTaskDisconnect)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  Future<process::Message> slaveRegisteredMessage =
    FUTURE_MESSAGE(Eq(SlaveRegisteredMessage().GetTypeName()), _, _);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);

  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 =
    StartSlave(detector.get(), &containerizer);
  ASSERT_SOME(slave1);

  // Capture the slaveRegisteredMessage which includes the slave pid.
  AWAIT_READY(slaveRegisteredMessage);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Submit a quota request for the given role.
  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();
  {
    Future<Quota> receivedSetRequest;
    EXPECT_CALL(allocator, setQuota(Eq(ROLE1), _))
      .WillOnce(DoAll(InvokeSetQuota(&allocator),
                      FutureArg<1>(&receivedSetRequest)));

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, /*force*/false));

    // Quota request succeeds and reaches the allocator.
    AWAIT_READY(receivedSetRequest);
    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Start a scheduler and a task.
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role(ROLE1);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched,
      frameworkInfo,
      master.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(_, _, _));

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  Offer offer = offers.get()[0];
  Resources taskResources = quotaResources;
  ASSERT_TRUE(Resources(offer.resources()).contains(taskResources));

  TaskInfo task = createTask(offers.get()[0], "", DEFAULT_EXECUTOR_ID);

  EXPECT_CALL(exec, registered(_, _, _, _));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  // Expect a TASK_RUNNING status, then a TASK_LOST after slave disconnect.
  Future<TaskStatus> runningStatus;
  Future<TaskStatus> lostStatus;

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&runningStatus))
    .WillOnce(FutureArg<1>(&lostStatus));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY(runningStatus);
  EXPECT_EQ(TASK_RUNNING, runningStatus.get().state());

  // Make sure deactivateSlave is called on allocator.
  Future<SlaveID> deactivatedSlaveID;
  EXPECT_CALL(allocator, deactivateSlave(_))
    .WillOnce(DoAll(InvokeDeactivateSlave(&allocator),
                    FutureArg<0>(&deactivatedSlaveID)));

  // Inject a slave exited event at the master causing the master
  // to mark the slave as disconnected.
  process::inject::exited(slaveRegisteredMessage.get().to, master.get()->pid);

  // Wait until allocator deactivates the slave.
  AWAIT_READY(deactivatedSlaveID);

  AWAIT_READY(lostStatus);
  EXPECT_EQ(TASK_LOST, lostStatus.get().state());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// Checks that an agent with quota'ed tasks disconnects and there are still
// enough free resources from another agent.
TEST_F(MasterQuotaTest, SufficientAfterSlaveWithQuotaTaskDisconnect)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));
  Future<process::Message> slaveRegisteredMessage =
    FUTURE_MESSAGE(Eq(SlaveRegisteredMessage().GetTypeName()), _, _);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);

  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 =
    StartSlave(detector.get(), &containerizer);

  ASSERT_SOME(slave1);

  // Capture the slaveRegisteredMessage which includes the slave pid.
  AWAIT_READY(slaveRegisteredMessage);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Submit a quota request for the given role.
  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();
  {
    Future<Quota> receivedSetRequest;
    EXPECT_CALL(allocator, setQuota(Eq(ROLE1), _))
      .WillOnce(DoAll(InvokeSetQuota(&allocator),
                      FutureArg<1>(&receivedSetRequest)));

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, /*force*/false));

    // Quota request succeeds and reaches the allocator.
    AWAIT_READY(receivedSetRequest);
    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Start a scheduler and a task.
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role(ROLE1);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched,
      frameworkInfo,
      master.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(_, _, _));

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_EQ(1u, offers.get().size());

  Offer offer = offers.get()[0];
  Resources taskResources = quotaResources;
  ASSERT_TRUE(Resources(offer.resources()).contains(taskResources));

  TaskInfo task = createTask(offers.get()[0], "", DEFAULT_EXECUTOR_ID);

  EXPECT_CALL(exec, registered(_, _, _, _));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  // Expect a TASK_RUNNING status, then a TASK_LOST after slave disconnect.
  Future<TaskStatus> runningStatus;
  Future<TaskStatus> lostStatus;

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&runningStatus))
    .WillOnce(FutureArg<1>(&lostStatus));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY(runningStatus);
  EXPECT_EQ(TASK_RUNNING, runningStatus.get().state());

  // Start another agent and wait until its resources are available.
  Future<Resources> agent2TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent2TotalResources)));

  // Capture slave2 id.
  Future<SlaveRegisteredMessage> slave2RegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  Try<Owned<cluster::Slave>> slave2 = StartSlave(detector.get());
  ASSERT_SOME(slave2);
  AWAIT_READY(slave2RegisteredMessage);
  AWAIT_READY(agent2TotalResources);

  // NOTE: we probably should forcefully trigger an allocation and make sure
  // no additional offer is sent to scheduler, because quota is already
  // satisfied.

  // Make sure deactivateSlave is called on allocator.
  Future<SlaveID> deactivatedSlaveID;
  EXPECT_CALL(allocator, deactivateSlave(_))
    .WillOnce(
      DoAll(
        InvokeDeactivateSlave(&allocator), FutureArg<0>(&deactivatedSlaveID)))
    .WillOnce(Return());  // Skip second deactivateSlave.

  // Inject a slave exited event at the master causing the master
  // to mark the slave as disconnected.
  process::inject::exited(slaveRegisteredMessage.get().to, master.get()->pid);

  // Wait until allocator deactivates the slave.
  AWAIT_READY(deactivatedSlaveID);

  AWAIT_READY(lostStatus);
  EXPECT_EQ(TASK_LOST, lostStatus.get().state());

  // Expect an offer to be sent for slave2.
  Future<vector<Offer>> offers2;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  AWAIT_READY(offers2);
  EXPECT_EQ(1u, offers2.get().size());
  EXPECT_EQ(slave2RegisteredMessage.get().slave_id(),
            offers2.get()[0].slave_id());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// Checks: two roles, two frameworks, one is production but rejects offers, the
// other is greedy and tries to hijack the cluster which is prevented by quota.
TEST_F(MasterQuotaTest, GreedyFrameworkCannotHijackQuotaRole)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  master::Flags flags = MesosTest::CreateMasterFlags();
  Try<Owned<cluster::Master>> master = StartMaster(&allocator, flags);
  ASSERT_SOME(master);

  // We request quota for all cpus and mem for the agent.
  Resources quotaResources =
    defaultAgentResources.filter(
        [=](const Resource& resource) {
          return (resource.name() == "cpus" || resource.name() == "mem");
        });

  const bool force = true;
  Future<Response> response = process::http::post(
      master.get()->pid,
      "quota",
      createBasicAuthHeaders(DEFAULT_CREDENTIAL),
      createRequestBody(ROLE1, quotaResources, force));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response) << response.get().body;

  // We create a "filtering" framework which declines every offer
  // it sees.
  FrameworkInfo frameworkInfo1 = createFrameworkInfo(ROLE1);
  MockScheduler sched1;
  MesosSchedulerDriver framework1(
      &sched1, frameworkInfo1, master.get()->pid, DEFAULT_CREDENTIAL);

  // Launch `framework1` in quota'ed role.
  Future<Nothing> registered1;
  EXPECT_CALL(sched1, registered(&framework1, _, _))
    .WillOnce(FutureSatisfy(&registered1));

  framework1.start();
  AWAIT_READY(registered1);

  // Launch framework2 in a different role.
  FrameworkInfo frameworkInfo2 = createFrameworkInfo(ROLE2);
  MockScheduler sched2;
  MesosSchedulerDriver framework2(
      &sched2, frameworkInfo2, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered2;
  EXPECT_CALL(sched2, registered(&framework2, _, _))
    .WillOnce(FutureSatisfy(&registered2));

  framework2.start();
  AWAIT_READY(registered2);

  // Start one agent and wait until its resources are available.
  Future<Resources> agent1TotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agent1TotalResources)));

  // The resource should be offered to framework1, then being declined.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched1, resourceOffers(&framework1, _))
    .WillOnce(FutureArg<1>(&offers));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  AWAIT_READY(agent1TotalResources);
  EXPECT_EQ(defaultAgentResources, agent1TotalResources.get());

  // Confirm that offer is sent at least once.
  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers.get().size());

  // Make sure allocator recovers the resource from decline.
  Future<Resources> declinedResources;
  EXPECT_CALL(allocator, recoverResources(_, _, _, _))
    .WillOnce(DoAll(InvokeRecoverResources(&allocator),
                    FutureArg<2>(&declinedResources)));

  // Now `framework` declines the offer and sets a filter
  // with the duration greater than the allocation interval.
  Duration filterTimeout = flags.allocation_interval * 2;
  Filters offerFilter;
  offerFilter.set_refuse_seconds(filterTimeout.secs());

  framework1.declineOffer(offers.get()[0].id(), offerFilter);

  AWAIT_READY(declinedResources);

  Clock::pause();

  // Ensure the offer filter timeout is set before advancing the clock.
  Clock::settle();

  // Resources on the agent should never be offered to framework2.
  EXPECT_CALL(sched2, resourceOffers(&framework2, _))
    .Times(0);

  // Trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched1, resourceOffers(&framework1, _))
    .WillOnce(FutureArg<1>(&offers2));
  EXPECT_CALL(allocator, recoverResources(_, _, _, _))
    .WillOnce(Return());

  // Trigger another batch allocation: filter timeout and we should
  // see another offer to `framework1`.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  AWAIT_READY(offers2);

  framework1.stop();
  framework1.join();

  framework2.stop();
  framework2.join();

  Clock::resume();
}


// Checks: multiple frameworks in quota'ed role and one framework in non
// quota'ed role, ensure total allocation satisfies quota, but may slightly
// exceed quota due to coarse-grained allocation.
TEST_F(MasterQuotaTest, TotalAllocationSatisfiesQuota)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Create three frameworks, put the first two in ROLE1, and the last in ROLE2.
  FrameworkInfo frameworkInfo1 = createFrameworkInfo(ROLE1);
  MockScheduler sched1;
  MesosSchedulerDriver framework1(
      &sched1, frameworkInfo1, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered1;
  EXPECT_CALL(sched1, registered(&framework1, _, _))
    .WillOnce(FutureSatisfy(&registered1));

  framework1.start();
  AWAIT_READY(registered1);

  FrameworkInfo frameworkInfo2 = createFrameworkInfo(ROLE1);
  MockScheduler sched2;
  MesosSchedulerDriver framework2(
      &sched2, frameworkInfo2, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered2;
  EXPECT_CALL(sched2, registered(&framework2, _, _))
    .WillOnce(FutureSatisfy(&registered2));

  framework2.start();
  AWAIT_READY(registered2);

  FrameworkInfo frameworkInfo3 = createFrameworkInfo(ROLE2);
  MockScheduler sched3;
  MesosSchedulerDriver framework3(
      &sched3, frameworkInfo3, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered3;
  EXPECT_CALL(sched3, registered(&framework3, _, _))
    .WillOnce(FutureSatisfy(&registered3));

  framework3.start();
  AWAIT_READY(registered3);

  // Forcefully request quota for more than one slave but less than two slaves.
  Resources agentResources =
    defaultAgentResources.filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    });
  Resources deltaResources = Resources::parse("cpus:1;mem:512").get();
  CHECK(agentResources.contains(deltaResources));
  Resources quotaResources = agentResources + deltaResources;

  const bool force = true;
  Future<Response> response = process::http::post(
      master.get()->pid,
      "quota",
      createBasicAuthHeaders(DEFAULT_CREDENTIAL),
      createRequestBody(ROLE1, quotaResources, force));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response) << response.get().body;

  // Setup offer expectation for all frameworks.
  Future<vector<Offer>> offers1;
  EXPECT_CALL(sched1, resourceOffers(&framework1, _))
    .WillOnce(FutureArg<1>(&offers1));

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched2, resourceOffers(&framework2, _))
    .WillOnce(FutureArg<1>(&offers2));

  Future<vector<Offer>> offers3;
  EXPECT_CALL(sched3, resourceOffers(&framework3, _))
    .WillOnce(FutureArg<1>(&offers3));

  // Make sure all three slaves are addded.
  Future<Resources> lastAgentResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .Times(3)
    .WillOnce(InvokeAddSlave(&allocator))
    .WillOnce(InvokeAddSlave(&allocator))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&lastAgentResources)));

  // Start three slaves.
  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  Try<Owned<cluster::Slave>> slave2 = StartSlave(detector.get());
  ASSERT_SOME(slave2);

  Try<Owned<cluster::Slave>> slave3 = StartSlave(detector.get());
  ASSERT_SOME(slave3);

  AWAIT_READY(lastAgentResources);

  AWAIT_READY(offers1);
  EXPECT_EQ(1u, offers1.get().size());

  AWAIT_READY(offers2);
  EXPECT_EQ(1u, offers2.get().size());

  Resources role1Resources =
    (offers1.get()[0].resources() + offers2.get()[0].resources())
    .filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    });

  // Because default allocator does not perform fine-grained allocation,
  // but instead allocate full agent, the total resources could exceed
  // quotaResources.
  EXPECT_EQ(agentResources + agentResources, role1Resources);
  EXPECT_TRUE(role1Resources.contains(quotaResources));

  AWAIT_READY(offers3);
  EXPECT_NE(0u, offers3.get().size());

  framework1.stop();
  framework1.join();

  framework2.stop();
  framework2.join();

  framework3.stop();
  framework3.join();
}


// Checks that removing existing quota when no task should not trigger
// offer rescind.
TEST_F(MasterQuotaTest, RemoveQuotaNoTask)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Make sure slave is registered.
  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  // Start a slave.
  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get());
  ASSERT_SOME(slave);

  AWAIT_READY(slaveRegisteredMessage);

  // Set a quota for role1.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Start a framework in role1.
  FrameworkInfo frameworkInfo = createFrameworkInfo(ROLE1);
  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureSatisfy(&registered));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();
  AWAIT_READY(registered);
  AWAIT_READY(offers);

  // Wrap the `http::requestDelete` into a lambda for readability of the test.
  auto removeQuota = [this, &master](const string& path) {
    return process::http::requestDelete(
        master.get()->pid,
        path,
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));
  };

  // Make sure no offerRescinded on framework.
  EXPECT_CALL(sched, offerRescinded(&driver, _))
    .Times(0);

  // Ensure we can remove the quota we have requested before.
  {
    Future<Nothing> receivedRemoveRequest;
    EXPECT_CALL(allocator, removeQuota(Eq(ROLE1)))
      .WillOnce(DoAll(InvokeRemoveQuota(&allocator),
                      FutureSatisfy(&receivedRemoveRequest)));

    Future<Response> response  = removeQuota("quota/" + ROLE1);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    // Ensure that the quota remove request has reached the allocator.
    AWAIT_READY(receivedRemoveRequest);
  }

  Clock::pause();
  Clock::settle();
  Clock::resume();

  driver.stop();
  driver.join();
}


// Checks that removing existing quota when some task uses the resource
// is fine.
TEST_F(MasterQuotaTest, RemoveQuotaWithTask)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Make sure slave is registered.
  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  // Start a slave.
  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);
  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), &containerizer);
  ASSERT_SOME(slave);

  AWAIT_READY(slaveRegisteredMessage);

  // Set a quota for role1.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Start a framework in role1.
  FrameworkInfo frameworkInfo = createFrameworkInfo(ROLE1);
  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureSatisfy(&registered));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();
  AWAIT_READY(registered);
  AWAIT_READY(offers);
  EXPECT_EQ(1u, offers.get().size());

  TaskInfo task = createTask(offers.get()[0], "", DEFAULT_EXECUTOR_ID);

  EXPECT_CALL(exec, registered(_, _, _, _));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> status;

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_RUNNING, status.get().state());

  // Wrap the `http::requestDelete` into a lambda for readability of the test.
  auto removeQuota = [this, &master](const string& path) {
    return process::http::requestDelete(
        master.get()->pid,
        path,
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));
  };

  // Ensure we can remove the quota we have requested before.
  {
    Future<Nothing> receivedRemoveRequest;
    EXPECT_CALL(allocator, removeQuota(Eq(ROLE1)))
      .WillOnce(DoAll(InvokeRemoveQuota(&allocator),
                      FutureSatisfy(&receivedRemoveRequest)));

    Future<Response> response  = removeQuota("quota/" + ROLE1);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    // Ensure that the quota remove request has reached the allocator.
    AWAIT_READY(receivedRemoveRequest);
  }

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// Checks that an update quota which increases the quota of role
// triggers proper offer rescind from competing roles until new
// quota is satisfied, while decreasing quota does not trigger
// rescind in current implementation.
TEST_F(MasterQuotaTest, UpdateQuotaRescindOffer)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  Try<Owned<cluster::Master>> master = StartMaster(&allocator);
  ASSERT_SOME(master);

  // Start three agents and wait until their resources are available.
  Future<Resources> agentTotalResources;
  EXPECT_CALL(allocator, addSlave(_, _, _, _, _))
    .Times(3)
    .WillOnce(InvokeAddSlave(&allocator))
    .WillOnce(InvokeAddSlave(&allocator))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<3>(&agentTotalResources)));

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave1 = StartSlave(detector.get());
  ASSERT_SOME(slave1);

  Try<Owned<cluster::Slave>> slave2 = StartSlave(detector.get());
  ASSERT_SOME(slave2);

  Try<Owned<cluster::Slave>> slave3 = StartSlave(detector.get());
  ASSERT_SOME(slave3);

  AWAIT_READY(agentTotalResources);

  // Our quota request requires more resources than available on the agent
  // (and in the cluster).
  Resources agentResource =
    agentTotalResources.get().filter([=](const Resource& resource) {
      return (resource.name() == "cpus" || resource.name() == "mem");
    });

  // Set a quota request for ROLE1 in place for one agent.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, agentResource));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Start one framework on quota'ed ROLE1 and one in non-quota'ed ROLE2.
  FrameworkInfo frameworkInfo1 = createFrameworkInfo(ROLE1);
  MockScheduler sched1;
  MesosSchedulerDriver framework1(
      &sched1, frameworkInfo1, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered1;
  EXPECT_CALL(sched1, registered(&framework1, _, _))
    .WillOnce(FutureSatisfy(&registered1));

  FrameworkInfo frameworkInfo2 = createFrameworkInfo(ROLE2);
  MockScheduler sched2;
  MesosSchedulerDriver framework2(
      &sched2, frameworkInfo2, master.get()->pid, DEFAULT_CREDENTIAL);

  Future<Nothing> registered2;
  EXPECT_CALL(sched2, registered(&framework2, _, _))
    .WillOnce(FutureSatisfy(&registered2));

  // Setup offer expectation for all frameworks.
  Future<vector<Offer>> offers1;
  Future<vector<Offer>> offers2;
  Future<vector<Offer>> offers3;
  Future<vector<Offer>> offers4;

  EXPECT_CALL(sched1, resourceOffers(&framework1, _))
    .WillOnce(FutureArg<1>(&offers1))
    .WillOnce(FutureArg<1>(&offers3))
    .WillOnce(FutureArg<1>(&offers4));

  EXPECT_CALL(sched2, resourceOffers(&framework2, _))
    .WillOnce(FutureArg<1>(&offers2));

  framework1.start();
  AWAIT_READY(registered1);
  AWAIT_READY(offers1);

  EXPECT_EQ(1u, offers1.get().size());

  framework2.start();
  AWAIT_READY(registered2);
  AWAIT_READY(offers2);

  EXPECT_EQ(2u, offers2.get().size());

  std::set<OfferID> offerIds;
  offerIds.insert(offers2.get()[0].id());
  offerIds.insert(offers2.get()[1].id());

  Future<OfferID> rescindedOffer1;
  Future<OfferID> rescindedOffer2;
  EXPECT_CALL(sched2, offerRescinded(&framework2, _))
    .WillOnce(FutureArg<1>(&rescindedOffer1))
    .WillOnce(FutureArg<1>(&rescindedOffer2));

  const bool force = false;

  // Increase the previously requested quota to two agents.
  // Master should only rescind one offer.
  {
    Resources quotaResources = agentResource + agentResource;

    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, force)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  AWAIT_READY(rescindedOffer1);
  AWAIT_READY(offers3);
  EXPECT_TRUE(rescindedOffer2.isPending());

  EXPECT_TRUE(offerIds.count(rescindedOffer1.get()) > 0);
  offerIds.erase(rescindedOffer1.get());
  EXPECT_EQ(1, offerIds.size());

  // Increase the previously requested quota to three agents.
  // Master should rescind one more offer.
  {
    Resources quotaResources = agentResource + agentResource + agentResource;

    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, force)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  AWAIT_READY(rescindedOffer2);
  AWAIT_READY(offers4);

  EXPECT_TRUE(offerIds.count(rescindedOffer2.get()) > 0);
  offerIds.erase(rescindedOffer2.get());
  EXPECT_EQ(0, offerIds.size());

  // Decrease the previously requested quota to two agents.
  // We don't actively rescind offer from over allocated quota'ed role now.
  {
    Resources quotaResources = agentResource + agentResource;

    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, force)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // TODO(zhitao): Consider decline some or all offers from framework1 here,
  // and verify that quota is still respected.

  framework1.stop();
  framework1.join();

  framework2.stop();
  framework2.join();
}


// These tests verify the behavior in presence of master failover and recovery.

// TODO(alexr): Tests to implement:
//   * During the recovery, no overcommitment of resources should happen.
//   * During the recovery, no allocation of resources potentially needed to
//     satisfy quota should happen.
//   * If a cluster is under quota before the failover, it should be under quota
//     during the recovery (total quota sanity check).
//   * Master fails simultaneously with multiple agents, rendering the cluster
//     under quota (total quota sanity check).


// These tests verify the authentication and authorization of quota requests.

// Checks that quota set and remove requests succeed if both authentication
// and authorization are disabled.
TEST_F(MasterQuotaTest, NoAuthenticationNoAuthorization)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  // Disable http_readwrite authentication and authorization.
  // TODO(alexr): Setting master `--acls` flag to `ACLs()` or `None()` seems
  // to be semantically equal, however, the test harness currently does not
  // allow `None()`. Once MESOS-4196 is resolved, use `None()` for clarity.
  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.acls = ACLs();
  masterFlags.authenticate_http_readwrite = false;

  Try<Owned<cluster::Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  // Check whether quota can be set.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Future<Quota> receivedSetRequest;
    EXPECT_CALL(allocator, setQuota(Eq(ROLE1), _))
      .WillOnce(DoAll(InvokeSetQuota(&allocator),
                      FutureArg<1>(&receivedSetRequest)));

    // Send a set quota request with absent credentials.
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        None(),
        createRequestBody(ROLE1, quotaResources, FORCE));

    // Quota request succeeds and reaches the allocator.
    AWAIT_READY(receivedSetRequest);
    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Check whether quota can be removed.
  {
    Future<Nothing> receivedRemoveRequest;
    EXPECT_CALL(allocator, removeQuota(Eq(ROLE1)))
      .WillOnce(DoAll(InvokeRemoveQuota(&allocator),
                      FutureSatisfy(&receivedRemoveRequest)));

    // Send a remove quota request with absent credentials.
    Future<Response> response = process::http::requestDelete(
        master.get()->pid,
        "quota/" + ROLE1,
        None());

    // Quota request succeeds and reaches the allocator.
    AWAIT_READY(receivedRemoveRequest);
    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that a set quota request is rejected for unauthenticated principals.
TEST_F(MasterQuotaTest, UnauthenticatedQuotaRequest)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // We do not need an agent since a request should be rejected before
  // we start looking at available resources.

  // A request can contain any amount of resources because it will be rejected
  // before we start looking at available resources.
  Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

  // The master is configured so that only requests from `DEFAULT_CREDENTIAL`
  // are authenticated.
  {
    Credential credential;
    credential.set_principal("unknown-principal");
    credential.set_secret("test-secret");

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(credential),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Unauthorized({}).status, response)
      << response.get().body;
  }

  // The absense of credentials leads to authentication failure as well.
  {
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        None(),
        createRequestBody(ROLE1, quotaResources));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Unauthorized({}).status, response)
      << response.get().body;
  }
}


// Checks that an authorized principal can set and remove quota while
// using get_quotas and update_quotas ACLs, while unauthorized user
// cannot.
TEST_F(MasterQuotaTest, AuthorizeGetUpdateQuotaRequests)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  // Setup ACLs so that only the default principal can modify quotas
  // for `ROLE1` and read status.
  ACLs acls;

  mesos::ACL::UpdateQuota* acl1 = acls.add_update_quotas();
  acl1->mutable_principals()->add_values(DEFAULT_CREDENTIAL.principal());
  acl1->mutable_roles()->add_values(ROLE1);

  mesos::ACL::UpdateQuota* acl2 = acls.add_update_quotas();
  acl2->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl2->mutable_roles()->set_type(mesos::ACL::Entity::NONE);

  mesos::ACL::GetQuota* acl3 = acls.add_get_quotas();
  acl3->mutable_principals()->add_values(DEFAULT_CREDENTIAL.principal());
  acl3->mutable_roles()->add_values(ROLE1);

  mesos::ACL::GetQuota* acl4 = acls.add_get_quotas();
  acl4->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl4->mutable_roles()->set_type(mesos::ACL::Entity::NONE);

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.acls = acls;

  Try<Owned<cluster::Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  // Try to update quota using a principal that is not the default principal.
  // This request will fail because only the default principal is authorized
  // to do that.
  {
    // As we don't care about the enforcement of quota but only the
    // authorization of the quota request we set the force flag in the post
    // request below to override the capacity heuristic check.
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL_2),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Forbidden().status, response)
      << response.get().body;
  }

  // Set quota using the default principal.
  {
    // As we don't care about the enforcement of quota but only the
    // authorization of the quota request we set the force flag in the post
    // request below to override the capacity heuristic check.
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();
    Future<Quota> quota;
    EXPECT_CALL(allocator, setQuota(Eq(ROLE1), _))
      .WillOnce(DoAll(InvokeSetQuota(&allocator),
                      FutureArg<1>(&quota)));

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    AWAIT_READY(quota);

    // Extract the principal from `DEFAULT_CREDENTIAL` because `EXPECT_EQ`
    // does not compile if `DEFAULT_CREDENTIAL.principal()` is used as an
    // argument.
    const string principal = DEFAULT_CREDENTIAL.principal();

    EXPECT_EQ(ROLE1, quota.get().info.role());
    EXPECT_EQ(principal, quota.get().info.principal());
    EXPECT_EQ(quotaResources, quota.get().info.guarantee());
  }

  // Try to get the previously requested quota using a princilal that is
  // not authorized to see it. This will result in empty information
  // returned.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL_2));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    EXPECT_EQ(0, status.get().infos().size());
  }

  // Get the previous requested quota using default principal, which is
  // authorized to see it.
  {
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    EXPECT_EQ(1, status.get().infos().size());
    EXPECT_EQ(ROLE1, status.get().infos(0).role());
  }

  // Try to update the previously requested quota using a principal that is
  // not the default principal. This will fail because only the default
  // principal is authorized to do that.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();
    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL_2),
            createRequestBody(ROLE1, quotaResources, FORCE)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Forbidden().status, response)
      << response.get().body;
  }

  // Update the previously requested quota using the default principal.
  {
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Future<Quota> quota;
    EXPECT_CALL(allocator, updateQuota(Eq(ROLE1), _))
      .WillOnce(DoAll(InvokeUpdateQuota(&allocator),
                      FutureArg<1>(&quota)));

    Future<Response> response = process::http::request(
        process::http::createRequest(
            master.get()->pid,
            "PUT",
            false,
            "quota",
            createBasicAuthHeaders(DEFAULT_CREDENTIAL),
            createRequestBody(ROLE1, quotaResources, FORCE)));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
    AWAIT_READY(quota);

    EXPECT_EQ(quotaResources, quota.get().info.guarantee());
  }

  // Try to remove the previously requested quota using a principal that is
  // not the default principal. This will fail because only the default
  // principal is authorized to do that.
  {
    Future<Response> response = process::http::requestDelete(
        master.get()->pid,
        "quota/" + ROLE1,
        createBasicAuthHeaders(DEFAULT_CREDENTIAL_2));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Forbidden().status, response)
      << response.get().body;
  }

  // Remove the previously requested quota using the default principal.
  {
    Future<Nothing> receivedRemoveRequest;
    EXPECT_CALL(allocator, removeQuota(Eq(ROLE1)))
      .WillOnce(DoAll(InvokeRemoveQuota(&allocator),
                      FutureSatisfy(&receivedRemoveRequest)));

    Future<Response> response = process::http::requestDelete(
        master.get()->pid,
        "quota/" + ROLE1,
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    AWAIT_READY(receivedRemoveRequest);
  }
}


// Checks that an authorized principal can update quota using deprecated
// set_quotas and remove_quotas, while unauthorized principals cannot.
//
// TODO(zhitao): Remove this test case at the end of deprecation cycle
// started with 1.0.
TEST_F(MasterQuotaTest, AuthorizeSetAndRemoveQuotaRequests)
{
  TestAllocator<> allocator;
  EXPECT_CALL(allocator, initialize(_, _, _, _, _));

  // Setup ACLs so that only the default principal can set and see
  // quotas for `ROLE1` and can remove its own quotas.
  ACLs acls;

  mesos::ACL::SetQuota* acl1 = acls.add_set_quotas();
  acl1->mutable_principals()->add_values(DEFAULT_CREDENTIAL.principal());
  acl1->mutable_roles()->add_values(ROLE1);

  mesos::ACL::SetQuota* acl2 = acls.add_set_quotas();
  acl2->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl2->mutable_roles()->set_type(mesos::ACL::Entity::NONE);

  mesos::ACL::RemoveQuota* acl3 = acls.add_remove_quotas();
  acl3->mutable_principals()->add_values(DEFAULT_CREDENTIAL.principal());
  acl3->mutable_quota_principals()->add_values(DEFAULT_CREDENTIAL.principal());

  mesos::ACL::RemoveQuota* acl4 = acls.add_remove_quotas();
  acl4->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl4->mutable_quota_principals()->set_type(mesos::ACL::Entity::NONE);

  mesos::ACL::GetQuota* acl5 = acls.add_get_quotas();
  acl5->mutable_principals()->add_values(DEFAULT_CREDENTIAL.principal());
  acl5->mutable_roles()->add_values(ROLE1);

  mesos::ACL::GetQuota* acl6 = acls.add_get_quotas();
  acl6->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl6->mutable_roles()->set_type(mesos::ACL::Entity::NONE);

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.acls = acls;

  Try<Owned<cluster::Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  // Try to request quota using a principal that is not the default principal.
  // This request will fail because only the default principal is authorized
  // to do that.
  {
    // As we don't care about the enforcement of quota but only the
    // authorization of the quota request we set the force flag in the post
    // request below to override the capacity heuristic check.
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL_2),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Forbidden().status, response)
      << response.get().body;
  }

  // Request quota using the default principal.
  {
    // As we don't care about the enforcement of quota but only the
    // authorization of the quota request we set the force flag in the post
    // request below to override the capacity heuristic check.
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    Future<Quota> quota;
    EXPECT_CALL(allocator, setQuota(Eq(ROLE1), _))
      .WillOnce(DoAll(InvokeSetQuota(&allocator),
                      FutureArg<1>(&quota)));

    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        createBasicAuthHeaders(DEFAULT_CREDENTIAL),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    AWAIT_READY(quota);

    // Extract the principal from `DEFAULT_CREDENTIAL` because `EXPECT_EQ`
    // does not compile if `DEFAULT_CREDENTIAL.principal()` is used as an
    // argument.
    const string principal = DEFAULT_CREDENTIAL.principal();

    EXPECT_EQ(ROLE1, quota.get().info.role());
    EXPECT_EQ(principal, quota.get().info.principal());
    EXPECT_EQ(quotaResources, quota.get().info.guarantee());
  }

  // Try to get the previously requested quota using a principal that is
  // not authorized to see it. This will result in empty information
  // returned.
  {
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL_2));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    EXPECT_EQ(0, status.get().infos().size());
  }

  // Get the previously requested quota using the default principal, which is
  // authorized to see it.
  {
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    EXPECT_EQ(1, status.get().infos().size());
    EXPECT_EQ(ROLE1, status.get().infos(0).role());
  }

  // Try to remove the previously requested quota using a principal that is
  // not the default principal. This will fail because only the default
  // principal is authorized to do that.
  {
    Future<Response> response = process::http::requestDelete(
        master.get()->pid,
        "quota/" + ROLE1,
        createBasicAuthHeaders(DEFAULT_CREDENTIAL_2));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Forbidden().status, response)
      << response.get().body;
  }

  // Remove the previously requested quota using the default principal.
  {
    Future<Nothing> receivedRemoveRequest;
    EXPECT_CALL(allocator, removeQuota(Eq(ROLE1)))
      .WillOnce(DoAll(InvokeRemoveQuota(&allocator),
                      FutureSatisfy(&receivedRemoveRequest)));

    Future<Response> response = process::http::requestDelete(
        master.get()->pid,
        "quota/" + ROLE1,
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    AWAIT_READY(receivedRemoveRequest);
  }
}


// Checks that get and update quota requests can be authorized without
// authentication if an authorization rule exists that applies to anyone.
// The authorizer will map the absence of a principal to "ANY".
TEST_F(MasterQuotaTest, AuthorizeGetUpdateQuotaRequestsWithoutPrincipal)
{
  // Setup ACLs so that any principal can set quotas for `ROLE1` and remove
  // anyone's quotas.
  ACLs acls;

  mesos::ACL::GetQuota* acl1 = acls.add_get_quotas();
  acl1->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl1->mutable_roles()->add_values(ROLE1);

  mesos::ACL::UpdateQuota* acl2 = acls.add_update_quotas();
  acl2->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl2->mutable_roles()->set_type(mesos::ACL::Entity::ANY);

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.acls = acls;
  masterFlags.authenticate_http_readonly = false;
  masterFlags.authenticate_http_readwrite = false;
  masterFlags.authenticate_http_frameworks = false;
  masterFlags.credentials = None();

  Try<Owned<cluster::Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  // Request quota without providing authorization headers.
  {
    // As we don't care about the enforcement of quota but only the
    // authorization of the quota request we set the force flag in the post
    // request below to override the capacity heuristic check.
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    // Create an HTTP request without authorization headers.
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        None(),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Get the previously requested quota without providing authoriation
  // headers.
  {
    Future<Response> response = process::http::get(
        master.get()->pid,
        "quota",
        None(),
        None());

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;

    EXPECT_SOME_EQ(
        "application/json",
        response.get().headers.get("Content-Type"));

    const Try<JSON::Object> parse =
      JSON::parse<JSON::Object>(response.get().body);

    ASSERT_SOME(parse);

    // Convert JSON response to `QuotaStatus` protobuf.
    const Try<QuotaStatus> status = ::protobuf::parse<QuotaStatus>(parse.get());
    ASSERT_FALSE(status.isError());

    EXPECT_EQ(1, status.get().infos().size());
    EXPECT_EQ(ROLE1, status.get().infos(0).role());
  }

  // Remove the previously requested quota without providing authorization
  // headers.
  {
    Future<Response> response = process::http::requestDelete(
        master.get()->pid,
        "quota/" + ROLE1,
        None());

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}


// Checks that set and remove quota requests can be authorized without
// authentication if an authorization rule exists that applies to anyone.
// The authorizer will map the absence of a principal to "ANY".
//
// TODO(zhitao): Remove this test case at the end of deprecation cycle
// started with 1.0.
TEST_F(MasterQuotaTest, AuthorizeSetRemoveQuotaRequestsWithoutPrincipal)
{
  // Setup ACLs so that any principal can set quotas for `ROLE1` and remove
  // anyone's quotas.
  ACLs acls;

  mesos::ACL::SetQuota* acl1 = acls.add_set_quotas();
  acl1->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl1->mutable_roles()->add_values(ROLE1);

  mesos::ACL::RemoveQuota* acl2 = acls.add_remove_quotas();
  acl2->mutable_principals()->set_type(mesos::ACL::Entity::ANY);
  acl2->mutable_quota_principals()->set_type(mesos::ACL::Entity::ANY);

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.acls = acls;
  masterFlags.authenticate_http_readonly = false;
  masterFlags.authenticate_http_readwrite = false;
  masterFlags.authenticate_http_frameworks = false;
  masterFlags.credentials = None();

  Try<Owned<cluster::Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  // Use the force flag for setting quota that cannot be satisfied in
  // this empty cluster without any agents.
  const bool FORCE = true;

  // Request quota without providing authorization headers.
  {
    // As we don't care about the enforcement of quota but only the
    // authorization of the quota request we set the force flag in the post
    // request below to override the capacity heuristic check.
    Resources quotaResources = Resources::parse("cpus:1;mem:512").get();

    // Create an HTTP request without authorization headers.
    Future<Response> response = process::http::post(
        master.get()->pid,
        "quota",
        None(),
        createRequestBody(ROLE1, quotaResources, FORCE));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }

  // Remove the previously requested quota without providing authorization
  // headers.
  {
    Future<Response> response = process::http::requestDelete(
        master.get()->pid,
        "quota/" + ROLE1,
        None());

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
      << response.get().body;
  }
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
