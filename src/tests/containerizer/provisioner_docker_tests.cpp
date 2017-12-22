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

#include <gmock/gmock.h>

#include <stout/duration.hpp>
#include <stout/gtest.hpp>
#include <stout/json.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/owned.hpp>

#include <mesos/docker/spec.hpp>

#ifdef __linux__
#include "linux/fs.hpp"
#endif // __linux__

#include "slave/containerizer/mesos/provisioner/backend.hpp"
#include "slave/containerizer/mesos/provisioner/constants.hpp"
#include "slave/containerizer/mesos/provisioner/paths.hpp"

#include "slave/containerizer/mesos/provisioner/docker/metadata_manager.hpp"
#include "slave/containerizer/mesos/provisioner/docker/paths.hpp"
#include "slave/containerizer/mesos/provisioner/docker/puller.hpp"
#include "slave/containerizer/mesos/provisioner/docker/registry_puller.hpp"
#include "slave/containerizer/mesos/provisioner/docker/store.hpp"

#include "tests/environment.hpp"
#include "tests/mesos.hpp"
#include "tests/utils.hpp"

#ifdef __linux__
#include "tests/containerizer/docker_archive.hpp"
#endif // __linux__

namespace master = mesos::internal::master;
namespace paths = mesos::internal::slave::docker::paths;
namespace slave = mesos::internal::slave;
namespace spec = ::docker::spec;

using std::string;
using std::vector;

using process::Future;
using process::Owned;
using process::PID;
using process::Promise;

using master::Master;

using mesos::internal::slave::AUFS_BACKEND;
using mesos::internal::slave::COPY_BACKEND;
using mesos::internal::slave::OVERLAY_BACKEND;
using mesos::internal::slave::Provisioner;

using mesos::master::detector::MasterDetector;

using slave::ImageInfo;
using slave::Slave;

using slave::docker::Puller;
using slave::docker::RegistryPuller;
using slave::docker::Store;

using testing::WithParamInterface;

namespace mesos {
namespace internal {
namespace tests {

class ProvisionerDockerLocalStoreTest : public TemporaryDirectoryTest
{
public:
  void verifyLocalDockerImage(
      const slave::Flags& flags,
      const vector<string>& layers)
  {
    // Verify contents of the image in store directory.
    const string layerPath1 = paths::getImageLayerRootfsPath(
        flags.docker_store_dir,
        "123",
        COPY_BACKEND);

    const string layerPath2 = paths::getImageLayerRootfsPath(
        flags.docker_store_dir,
        "456",
        COPY_BACKEND);

    EXPECT_TRUE(os::exists(layerPath1));
    EXPECT_TRUE(os::exists(layerPath2));

    EXPECT_SOME_EQ(
        "foo 123",
        os::read(path::join(layerPath1, "temp")));

    EXPECT_SOME_EQ(
        "bar 456",
        os::read(path::join(layerPath2, "temp")));

    // Verify the Docker Image provided.
    vector<string> expectedLayers;
    expectedLayers.push_back(layerPath1);
    expectedLayers.push_back(layerPath2);
    EXPECT_EQ(expectedLayers, layers);
  }

protected:
  virtual void SetUp()
  {
    TemporaryDirectoryTest::SetUp();

    const string archivesDir = path::join(os::getcwd(), "images");
    const string image = path::join(archivesDir, "abc");
    ASSERT_SOME(os::mkdir(archivesDir));
    ASSERT_SOME(os::mkdir(image));

    JSON::Value repositories = JSON::parse(
        "{"
        "  \"abc\": {"
        "    \"latest\": \"456\""
        "  }"
        "}").get();
    ASSERT_SOME(
        os::write(path::join(image, "repositories"), stringify(repositories)));

    ASSERT_SOME(os::mkdir(path::join(image, "123")));
    JSON::Value manifest123 = JSON::parse(
        "{"
        "  \"parent\": \"\""
        "}").get();
    ASSERT_SOME(os::write(
        path::join(image, "123", "json"), stringify(manifest123)));
    ASSERT_SOME(os::mkdir(path::join(image, "123", "layer")));
    ASSERT_SOME(
        os::write(path::join(image, "123", "layer", "temp"), "foo 123"));

    // Must change directory to avoid carrying over /path/to/archive during tar.
    const string cwd = os::getcwd();
    ASSERT_SOME(os::chdir(path::join(image, "123", "layer")));
    ASSERT_SOME(os::tar(".", "../layer.tar"));
    ASSERT_SOME(os::chdir(cwd));
    ASSERT_SOME(os::rmdir(path::join(image, "123", "layer")));

    ASSERT_SOME(os::mkdir(path::join(image, "456")));
    JSON::Value manifest456 = JSON::parse(
        "{"
        "  \"parent\": \"123\""
        "}").get();
    ASSERT_SOME(
        os::write(path::join(image, "456", "json"), stringify(manifest456)));
    ASSERT_SOME(os::mkdir(path::join(image, "456", "layer")));
    ASSERT_SOME(
        os::write(path::join(image, "456", "layer", "temp"), "bar 456"));

    ASSERT_SOME(os::chdir(path::join(image, "456", "layer")));
    ASSERT_SOME(os::tar(".", "../layer.tar"));
    ASSERT_SOME(os::chdir(cwd));
    ASSERT_SOME(os::rmdir(path::join(image, "456", "layer")));

    ASSERT_SOME(os::chdir(image));
    ASSERT_SOME(os::tar(".", "../abc.tar"));
    ASSERT_SOME(os::chdir(cwd));
    ASSERT_SOME(os::rmdir(image));
  }
};


// This test verifies that a locally stored Docker image in the form of a
// tar archive created from a 'docker save' command can be unpacked and
// stored in the proper locations accessible to the Docker provisioner.
TEST_F(ProvisionerDockerLocalStoreTest, LocalStoreTestWithTar)
{
  slave::Flags flags;
  flags.docker_registry = path::join(os::getcwd(), "images");
  flags.docker_store_dir = path::join(os::getcwd(), "store");
  flags.image_provisioner_backend = COPY_BACKEND;

  Try<Owned<slave::Store>> store = slave::docker::Store::create(flags);
  ASSERT_SOME(store);

  Image mesosImage;
  mesosImage.set_type(Image::DOCKER);
  mesosImage.mutable_docker()->set_name("abc");

  Future<slave::ImageInfo> imageInfo =
    store.get()->get(mesosImage, COPY_BACKEND);

  AWAIT_READY(imageInfo);

  verifyLocalDockerImage(flags, imageInfo->layers);
}


// This tests the ability of the metadata manger to recover the images it has
// already stored on disk when it is initialized.
TEST_F(ProvisionerDockerLocalStoreTest, MetadataManagerInitialization)
{
  slave::Flags flags;
  flags.docker_registry = path::join(os::getcwd(), "images");
  flags.docker_store_dir = path::join(os::getcwd(), "store");
  flags.image_provisioner_backend = COPY_BACKEND;

  Try<Owned<slave::Store>> store = slave::docker::Store::create(flags);
  ASSERT_SOME(store);

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("abc");

  Future<slave::ImageInfo> imageInfo = store.get()->get(image, COPY_BACKEND);
  AWAIT_READY(imageInfo);

  // Store is deleted and recreated. Metadata Manager is initialized upon
  // creation of the store.
  store->reset();
  store = slave::docker::Store::create(flags);
  ASSERT_SOME(store);
  Future<Nothing> recover = store.get()->recover();
  AWAIT_READY(recover);

  imageInfo = store.get()->get(image, COPY_BACKEND);
  AWAIT_READY(imageInfo);
  verifyLocalDockerImage(flags, imageInfo->layers);
}


// This test verifies that the layer that is missing from the store
// will be pulled.
TEST_F(ProvisionerDockerLocalStoreTest, MissingLayer)
{
  slave::Flags flags;
  flags.docker_registry = path::join(os::getcwd(), "images");
  flags.docker_store_dir = path::join(os::getcwd(), "store");
  flags.image_provisioner_backend = COPY_BACKEND;

  Try<Owned<slave::Store>> store = Store::create(flags);
  ASSERT_SOME(store);

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("abc");

  Future<slave::ImageInfo> imageInfo = store.get()->get(
      image, flags.image_provisioner_backend.get());

  AWAIT_READY(imageInfo);
  verifyLocalDockerImage(flags, imageInfo->layers);

  // Remove one of the layers from the store.
  ASSERT_SOME(os::rmdir(paths::getImageLayerRootfsPath(
      flags.docker_store_dir, "456", flags.image_provisioner_backend.get())));

  // Pull the image again to get the missing layer.
  imageInfo = store.get()->get(image, flags.image_provisioner_backend.get());
  AWAIT_READY(imageInfo);
  verifyLocalDockerImage(flags, imageInfo->layers);
}


class MockPuller : public Puller
{
public:
  MockPuller()
  {
    EXPECT_CALL(*this, pull(_, _, _, _))
      .WillRepeatedly(Invoke(this, &MockPuller::unmocked_pull));
  }

  virtual ~MockPuller() {}

  MOCK_METHOD4(
      pull,
      Future<vector<string>>(
          const spec::ImageReference&,
          const string&,
          const string&,
          const Option<Secret>&));

  Future<vector<string>> unmocked_pull(
      const spec::ImageReference& reference,
      const string& directory,
      const string& backend,
      const Option<Secret>& config)
  {
    // TODO(gilbert): Allow return list to be overridden.
    return vector<string>();
  }
};


// This tests the store to pull the same image simultaneously.
// This test verifies that the store only calls the puller once
// when multiple requests for the same image is in flight.
TEST_F(ProvisionerDockerLocalStoreTest, PullingSameImageSimutanuously)
{
  slave::Flags flags;
  flags.docker_registry = path::join(os::getcwd(), "images");
  flags.docker_store_dir = path::join(os::getcwd(), "store");

  MockPuller* puller = new MockPuller();
  Future<Nothing> pull;
  Future<string> directory;
  Promise<vector<string>> promise;

  EXPECT_CALL(*puller, pull(_, _, _, _))
    .WillOnce(testing::DoAll(FutureSatisfy(&pull),
                             FutureArg<1>(&directory),
                             Return(promise.future())));

  Try<Owned<slave::Store>> store =
      slave::docker::Store::create(flags, Owned<Puller>(puller));
  ASSERT_SOME(store);

  Image mesosImage;
  mesosImage.set_type(Image::DOCKER);
  mesosImage.mutable_docker()->set_name("abc");

  Future<slave::ImageInfo> imageInfo1 =
    store.get()->get(mesosImage, COPY_BACKEND);

  AWAIT_READY(pull);
  AWAIT_READY(directory);

  // TODO(gilbert): Need a helper method to create test layers
  // which will allow us to set manifest so that we can add
  // checks here.
  const string layerPath = path::join(directory.get(), "456");

  Try<Nothing> mkdir = os::mkdir(layerPath);
  ASSERT_SOME(mkdir);

  JSON::Value manifest = JSON::parse(
        "{"
        "  \"parent\": \"\""
        "}").get();

  ASSERT_SOME(
      os::write(path::join(layerPath, "json"), stringify(manifest)));

  ASSERT_TRUE(imageInfo1.isPending());
  Future<slave::ImageInfo> imageInfo2 =
    store.get()->get(mesosImage, COPY_BACKEND);

  const vector<string> result = {"456"};

  ASSERT_TRUE(imageInfo2.isPending());
  promise.set(result);

  AWAIT_READY(imageInfo1);
  AWAIT_READY(imageInfo2);

  EXPECT_EQ(imageInfo1->layers, imageInfo2->layers);
}


// This tests pruning images for docker store, especially that
// active images and layers will not be pruned.
TEST_F(ProvisionerDockerLocalStoreTest, PruneImages)
{
  slave::Flags flags;
  flags.docker_registry = path::join(os::getcwd(), "images");
  flags.docker_store_dir = path::join(os::getcwd(), "store");
  flags.image_provisioner_backend = COPY_BACKEND;

  Try<Owned<slave::Store>> store = Store::create(flags);
  ASSERT_SOME(store);

  const string imageName = "abc";

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name(imageName);

  Future<slave::ImageInfo> imageInfo = store.get()->get(
      image, flags.image_provisioner_backend.get());

  AWAIT_READY(imageInfo);
  verifyLocalDockerImage(flags, imageInfo->layers);

  vector<Image> excludedImages{image};

  // PruneImages with same excludedImages call should
  // keep the layers around.
  Future<Nothing> prune = store.get()->prune(excludedImages, {});
  AWAIT_READY(prune);

  // The layers should still be there for verify.
  verifyLocalDockerImage(flags, imageInfo->layers);

  const string layerPath1 =
    paths::getImageLayerRootfsPath(flags.docker_store_dir, "123", COPY_BACKEND);

  const string layerPath2 =
    paths::getImageLayerRootfsPath(flags.docker_store_dir, "456", COPY_BACKEND);

  // Keep all layers around through `activeLayerPaths`.
  Future<Nothing> prune2 = store.get()->prune({}, {layerPath1, layerPath2});
  AWAIT_READY(prune2);
  verifyLocalDockerImage(flags, imageInfo->layers);

  // Do not keep any image or layer, this should result
  // in an empty layers directory.
  Future<Nothing> prune3 = store.get()->prune({}, {});
  AWAIT_READY(prune3);

  EXPECT_FALSE(os::exists(layerPath1));

  // We can still get the image, but it need to pull again.
  Future<slave::ImageInfo> imageInfo3 = store.get()->get(
      image, flags.image_provisioner_backend.get());
  AWAIT_READY(imageInfo3);
}

#ifdef __linux__
class ProvisionerDockerTest
  : public MesosTest,
    public WithParamInterface<string> {};


// This test verifies that local docker image can be pulled and
// provisioned correctly, and shell command should be executed.
TEST_F(ProvisionerDockerTest, ROOT_LocalPullerSimpleCommand)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  const string directory = path::join(os::getcwd(), "archives");

  Future<Nothing> testImage = DockerArchive::create(directory, "alpine");
  AWAIT_READY(testImage);

  ASSERT_TRUE(os::exists(path::join(directory, "alpine.tar")));

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";
  flags.docker_registry = directory;
  flags.docker_store_dir = path::join(os::getcwd(), "store");

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  const Offer& offer = offers.get()[0];

  TaskInfo task = createTask(
      offer.slave_id(),
      Resources::parse("cpus:1;mem:128").get(),
      "ls -al /");

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("alpine");

  ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::MESOS);
  container->mutable_mesos()->mutable_image()->CopyFrom(image);

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY_FOR(statusStarting, Seconds(60));
  EXPECT_EQ(task.task_id(), statusStarting->task_id());
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY_FOR(statusRunning, Seconds(60));
  EXPECT_EQ(task.task_id(), statusRunning->task_id());
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(task.task_id(), statusFinished->task_id());
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  driver.stop();
  driver.join();
}


// For official Docker images, users can omit the 'library/' prefix
// when specifying the repository name (e.g., 'busybox'). The registry
// puller normalize docker official images if necessary.
INSTANTIATE_TEST_CASE_P(
    ImageAlpine,
    ProvisionerDockerTest,
    ::testing::ValuesIn(vector<string>({
        "alpine", // Verifies the normalization of the Docker repository name.
        "library/alpine",
        // TODO(alexr): The registry below is unreliable and hence disabled.
        // Consider re-enabling shall it become more stable.
        // "registry.cn-hangzhou.aliyuncs.com/acs-sample/alpine",
        "quay.io/coreos/alpine-sh"})));


// TODO(jieyu): This is a ROOT test because of MESOS-4757. Remove the
// ROOT restriction after MESOS-4757 is resolved.
TEST_P(ProvisionerDockerTest, ROOT_INTERNET_CURL_SimpleCommand)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";

  // Image pulling time may be long, depending on the location of
  // the registry server.
  flags.executor_registration_timeout = Minutes(10);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  const Offer& offer = offers.get()[0];

  // NOTE: We use a non-shell command here because 'sh' might not be
  // in the PATH. 'alpine' does not specify env PATH in the image. On
  // some linux distribution, '/bin' is not in the PATH by default.
  CommandInfo command;
  command.set_shell(false);
  command.set_value("/bin/ls");
  command.add_arguments("ls");
  command.add_arguments("-al");
  command.add_arguments("/");

  TaskInfo task = createTask(
      offer.slave_id(),
      Resources::parse("cpus:1;mem:128").get(),
      command);

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name(GetParam());

  ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::MESOS);
  container->mutable_mesos()->mutable_image()->CopyFrom(image);

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY_FOR(statusStarting, Minutes(10));
  EXPECT_EQ(task.task_id(), statusStarting->task_id());
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(task.task_id(), statusRunning->task_id());
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(task.task_id(), statusFinished->task_id());
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  driver.stop();
  driver.join();
}


// This test verifies that the scratch based docker image (that
// only contain a single binary and its dependencies) can be
// launched correctly.
TEST_F(ProvisionerDockerTest, ROOT_INTERNET_CURL_ScratchImage)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  const Offer& offer = offers.get()[0];

  CommandInfo command;
  command.set_shell(false);

  TaskInfo task = createTask(
      offer.slave_id(),
      Resources::parse("cpus:1;mem:128").get(),
      command);

  Image image;
  image.set_type(Image::DOCKER);

  // 'hello-world' is a scratch image. It contains only one
  // binary 'hello' in its rootfs.
  image.mutable_docker()->set_name("hello-world");

  ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::MESOS);
  container->mutable_mesos()->mutable_image()->CopyFrom(image);

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY_FOR(statusStarting, Seconds(60));
  EXPECT_EQ(task.task_id(), statusStarting->task_id());
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY_FOR(statusRunning, Seconds(60));
  EXPECT_EQ(task.task_id(), statusRunning->task_id());
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(task.task_id(), statusFinished->task_id());
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  driver.stop();
  driver.join();
}


class ProvisionerDockerBackendTest
  : public MesosTest,
    public WithParamInterface<string>
{
public:
  // Returns the supported backends.
  static vector<string> parameters()
  {
    vector<string> backends = {COPY_BACKEND};

    Try<bool> aufsSupported = fs::supported("aufs");
    if (aufsSupported.isSome() && aufsSupported.get()) {
      backends.push_back(AUFS_BACKEND);
    }

    Try<bool> overlayfsSupported = fs::supported("overlayfs");
    if (overlayfsSupported.isSome() && overlayfsSupported.get()) {
      backends.push_back(OVERLAY_BACKEND);
    }

    return backends;
  }
};


INSTANTIATE_TEST_CASE_P(
    BackendFlag,
    ProvisionerDockerBackendTest,
    ::testing::ValuesIn(ProvisionerDockerBackendTest::parameters()));


// This test verifies that a docker image containing whiteout files
// will be processed correctly by copy, aufs and overlay backends.
TEST_P(ProvisionerDockerBackendTest, ROOT_INTERNET_CURL_DTYPE_Whiteout)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";
  flags.image_provisioner_backend = GetParam();

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  const Offer& offer = offers.get()[0];

  // We are using the docker image 'zhq527725/whiteout' to verify that the
  // files '/dir1/file1' and '/dir1/dir2/file2' do not exist in container's
  // rootfs because of the following two whiteout files in the image:
  //   '/dir1/.wh.file1'
  //   '/dir1/dir2/.wh..wh..opq'
  // And we also verify that the file '/dir1/dir2/file3' exists in container's
  // rootfs which will NOT be applied by '/dir1/dir2/.wh..wh..opq' since they
  // are in the same layer.
  // See more details about this docker image in the link below:
  //   https://hub.docker.com/r/zhq527725/whiteout/
  CommandInfo command = createCommandInfo(
      "test ! -f /dir1/file1 && "
      "test ! -f /dir1/dir2/file2 && "
      "test -f /dir1/dir2/file3");

  TaskInfo task = createTask(
      offer.slave_id(),
      Resources::parse("cpus:1;mem:128").get(),
      command);

  Image image = createDockerImage("zhq527725/whiteout");

  ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::MESOS);
  container->mutable_mesos()->mutable_image()->CopyFrom(image);

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY_FOR(statusStarting, Seconds(60));
  EXPECT_EQ(task.task_id(), statusStarting->task_id());
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(task.task_id(), statusRunning->task_id());
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(task.task_id(), statusFinished->task_id());
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  driver.stop();
  driver.join();
}


// This test verifies that the provisioner correctly overwrites a
// directory in underlying layers with a with a regular file or symbolic
// link of the same name in an upper layer, and vice versa.
TEST_P(ProvisionerDockerBackendTest, ROOT_INTERNET_CURL_DTYPE_Overwrite)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";
  flags.image_provisioner_backend = GetParam();

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  const Offer& offer = offers.get()[0];

  // We are using the docker image 'chhsiao/overwrite' to verify that:
  //   1. The '/merged' directory is merged.
  //   2. All '/replaced*' files/directories are correctly overwritten.
  //   3. The '/foo', '/bar' and '/baz' files are correctly overwritten.
  // See more details in the following link:
  //   https://hub.docker.com/r/chhsiao/overwrite/
  CommandInfo command = createCommandInfo(
      "test -f /replaced1 &&"
      "test -L /replaced2 &&"
      "test -f /replaced2/m1 &&"
      "test -f /replaced2/m2 &&"
      "! test -e /replaced2/r2 &&"
      "test -d /replaced3 &&"
      "test -d /replaced4 &&"
      "! test -e /replaced4/m1 &&"
      "test -f /foo &&"
      "! test -L /bar &&"
      "test -L /baz");

  TaskInfo task = createTask(
      offer.slave_id(),
      Resources::parse("cpus:1;mem:128").get(),
      command);

  Image image = createDockerImage("chhsiao/overwrite");

  ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::MESOS);
  container->mutable_mesos()->mutable_image()->CopyFrom(image);

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  // Create a non-empty file 'abc' in the agent's work directory on the
  // host filesystem. This file is symbolically linked by
  //   '/xyz -> ../../../../../../../abc'
  // in the 2nd layer of the testing image during provisioning.
  // For more details about the provisioner directory please see:
  //   https://github.com/apache/mesos/blob/master/src/slave/containerizer/mesos/provisioner/paths.hpp#L34-L48 // NOLINT
  const string hostFile = path::join(flags.work_dir, "abc");
  ASSERT_SOME(os::write(hostFile, "abc"));
  ASSERT_SOME(os::shell("test -s " + hostFile));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY_FOR(statusStarting, Seconds(60));
  EXPECT_EQ(task.task_id(), statusStarting->task_id());
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY_FOR(statusRunning, Seconds(60));
  EXPECT_EQ(task.task_id(), statusRunning->task_id());
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(task.task_id(), statusFinished->task_id());
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  // The non-empty file should not be overwritten by the empty file
  // '/xyz' in the 3rd layer of the testing image during provisioning.
  EXPECT_SOME(os::shell("test -s " + hostFile));

  driver.stop();
  driver.join();
}


// This test verifies that Docker image can be pulled from the
// repository by digest.
TEST_F(ProvisionerDockerTest, ROOT_INTERNET_CURL_ImageDigest)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  const Offer& offer = offers.get()[0];

  // NOTE: We use a non-shell command here because 'sh' might not be
  // in the PATH. 'alpine' does not specify env PATH in the image. On
  // some linux distribution, '/bin' is not in the PATH by default.
  CommandInfo command;
  command.set_shell(false);
  command.set_value("/bin/ls");
  command.add_arguments("ls");
  command.add_arguments("-al");
  command.add_arguments("/");

  TaskInfo task = createTask(
      offer.slave_id(),
      Resources::parse("cpus:1;mem:128").get(),
      command);

  // NOTE: We use the digest of the 'alpine:2.7' image because it has a
  // Schema 1 manifest (the only manifest schema that we currently support).
  const string digest =
    "sha256:9f08005dff552038f0ad2f46b8e65ff3d25641747d3912e3ea8da6785046561a";

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("library/alpine@" + digest);

  ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::MESOS);
  container->mutable_mesos()->mutable_image()->CopyFrom(image);

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY_FOR(statusStarting, Seconds(60));
  EXPECT_EQ(task.task_id(), statusStarting->task_id());
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY_FOR(statusRunning, Seconds(60));
  EXPECT_EQ(task.task_id(), statusRunning->task_id());
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(task.task_id(), statusFinished->task_id());
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  driver.stop();
  driver.join();
}


// This test verifies that if a container image is specified, the
// command runs as the specified user 'nobody' and the sandbox of
// the command task is writeable by the specified user. It also
// verifies that stdout/stderr are owned by the specified user.
TEST_F(ProvisionerDockerTest, ROOT_INTERNET_CURL_CommandTaskUser)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  const Offer& offer = offers.get()[0];

  Result<uid_t> uid = os::getuid("nobody");
  ASSERT_SOME(uid);

  CommandInfo command;
  command.set_user("nobody");
  command.set_value(strings::format(
      "#!/bin/sh\n"
      "touch $MESOS_SANDBOX/file\n"
      "FILE_UID=`stat -c %%u $MESOS_SANDBOX/file`\n"
      "test $FILE_UID = %d\n"
      "STDOUT_UID=`stat -c %%u $MESOS_SANDBOX/stdout`\n"
      "test $STDOUT_UID = %d\n"
      "STDERR_UID=`stat -c %%u $MESOS_SANDBOX/stderr`\n"
      "test $STDERR_UID = %d\n",
      uid.get(), uid.get(), uid.get()).get());

  TaskInfo task = createTask(
      offer.slave_id(),
      Resources::parse("cpus:1;mem:128").get(),
      command);

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("alpine");

  ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::MESOS);
  container->mutable_mesos()->mutable_image()->CopyFrom(image);

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY_FOR(statusStarting, Seconds(60));
  EXPECT_EQ(task.task_id(), statusStarting->task_id());
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY_FOR(statusRunning, Seconds(60));
  EXPECT_EQ(task.task_id(), statusRunning->task_id());
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(task.task_id(), statusFinished->task_id());
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());

  driver.stop();
  driver.join();
}


// This test simulate the case that after the agent reboots the
// container runtime directory is gone while the provisioner
// directory still survives. The recursive `provisioner::destroy()`
// can make sure that a child container is always cleaned up
// before its parent container.
TEST_F(ProvisionerDockerTest, ROOT_RecoverNestedOnReboot)
{
  const string directory = path::join(os::getcwd(), "archives");

  Future<Nothing> testImage = DockerArchive::create(directory, "alpine");
  AWAIT_READY(testImage);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";
  flags.docker_registry = directory;
  flags.docker_store_dir = path::join(os::getcwd(), "store");

  Try<Owned<Provisioner>> provisioner = Provisioner::create(flags);
  ASSERT_SOME(provisioner);

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  ContainerID nestedContainerId;
  nestedContainerId.mutable_parent()->CopyFrom(containerId);
  nestedContainerId.set_value(id::UUID::random().toString());

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("alpine");

  AWAIT_READY(provisioner.get()->provision(nestedContainerId, image));

  // Passing an empty hashset to `provisioner::recover()` to
  // simulate the agent reboot scenario.
  AWAIT_READY(provisioner.get()->recover({}));

  const string containerDir = slave::provisioner::paths::getContainerDir(
      slave::paths::getProvisionerDir(flags.work_dir),
      containerId);

  EXPECT_FALSE(os::exists(containerDir));
}

#endif // __linux__

// This test verifies that provisioner checkpoints layers from provision
// call, recovers them after agent recovery, and able to pass the layers
// to image stores.
TEST_F(ProvisionerDockerTest, RecoverLayersForPrune)
{
  const string directory = path::join(os::getcwd(), "archives");

  Future<Nothing> testImage = DockerArchive::create(directory, "alpine");
  AWAIT_READY(testImage);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";
  flags.image_provisioner_backend = COPY_BACKEND;
  flags.docker_registry = directory;
  flags.docker_store_dir = path::join(os::getcwd(), "store");

  Try<Owned<Provisioner>> provisioner = Provisioner::create(flags);
  ASSERT_SOME(provisioner);

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("alpine");

  AWAIT_READY(provisioner.get()->provision(containerId, image));

  const string layersPath = slave::provisioner::paths::getLayersFilePath(
      slave::paths::getProvisionerDir(flags.work_dir),
      containerId);

  EXPECT_TRUE(os::exists(layersPath));

  // Reset provisioner.
  provisioner = Provisioner::create(flags);
  ASSERT_SOME(provisioner);

  AWAIT_READY(provisioner.get()->recover({containerId}));

  // The prune call should not prune any layer.
  Future<Nothing> prune = provisioner.get()->pruneImages({});

  AWAIT_READY(prune);

  // Verify that layers in docker store are not empty.
  Try<std::list<string>> layers =
    slave::docker::paths::listLayers(flags.docker_store_dir);
  ASSERT_SOME(layers);
  ASSERT_NE(layers->size(), 0);
}

// TODO(zhitao): Add a test to check that when layer information
// for a container is not properly recovered, `pruneImages` call is
// failed.

class MockStore: public slave::Store
{
public:
  MockStore() {}

  virtual ~MockStore() {}

  MOCK_METHOD0(
      recover,
      Future<Nothing>());

  MOCK_METHOD2(
      get,
      Future<ImageInfo>(
          const Image&,
          const string&));

  MOCK_METHOD2(
      prune,
      Future<Nothing>(
          const vector<Image>&,
          const hashset<string>&));
};


// This test verifies that pruneImages call are blocked until active
// provision finishes, but multiple provision requests can happen
// at the same time, because of the RWMutex used to protect provisoiner.
TEST_F(ProvisionerDockerTest, PruneBlocksOnProvision)
{
  const string directory = path::join(os::getcwd(), "archives");

  Future<Nothing> testImage = DockerArchive::create(directory, "alpine");
  AWAIT_READY(testImage);

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "docker/runtime,filesystem/linux";
  flags.image_providers = "docker";
  flags.image_provisioner_backend = COPY_BACKEND;
  flags.docker_registry = directory;
  flags.docker_store_dir = path::join(os::getcwd(), "store");

  hashmap<string, Owned<slave::Backend>> backends =
    slave::Backend::create(flags);

  Try<Owned<slave::Store>> store = slave::docker::Store::create(flags);
  ASSERT_SOME(store);

  MockStore* mockStore = new MockStore();

  hashmap<Image::Type, process::Owned<slave::Store>> stores{
    {Image::DOCKER, Owned<slave::Store>(mockStore)}
  };

  const string rootDir = slave::paths::getProvisionerDir(flags.work_dir);

  Owned<slave::ProvisionerProcess> process(
      new slave::ProvisionerProcess(
          rootDir,
          COPY_BACKEND,
          stores,
          backends));

  Owned<Provisioner> provisioner(new Provisioner(process));

  ContainerID containerId1;
  containerId1.set_value("container1");

  ContainerID containerId2;
  containerId2.set_value("container2");

  ContainerID containerId3;
  containerId3.set_value("container3");

  Image image;
  image.set_type(Image::DOCKER);
  image.mutable_docker()->set_name("alpine");

  Future<ImageInfo> imageInfo = store.get()->get(image, COPY_BACKEND);
  AWAIT_READY(imageInfo);

  Promise<ImageInfo> getPromise;
  Promise<Nothing> prunePromise;

  // Block the first provision while allow the second to finish.
  EXPECT_CALL(*mockStore, get(_, _))
    .WillOnce(Return(getPromise.future()))
    .WillOnce(Return(imageInfo))
    .WillOnce(Return(imageInfo));

  EXPECT_CALL(*mockStore, prune(_, _))
    .WillOnce(Return(prunePromise.future()));

  Future<slave::ProvisionInfo> provision1 =
    provisioner->provision(containerId1, image);

  Future<slave::ProvisionInfo> provision2 =
    provisioner->provision(containerId2, image);

  Future<Nothing> prune =
    provisioner->pruneImages({image});

  Future<slave::ProvisionInfo> provision3 =
    provisioner->provision(containerId3, image);

  AWAIT_READY(provision2);

  ASSERT_FALSE(provision1.isReady());
  ASSERT_FALSE(prune.isReady());
  ASSERT_FALSE(provision3.isReady());

  getPromise.set(imageInfo.get());

  AWAIT_READY(provision1);

  ASSERT_FALSE(prune.isReady());
  ASSERT_FALSE(provision3.isReady());

  prunePromise.set(Nothing());

  AWAIT_READY(prune);
  AWAIT_READY(provision3);
}

#endif

} // namespace tests {
} // namespace internal {
} // namespace mesos {
