import subprocess
import os
import time
import glob
import sys
import math

try:
    sys.path.append(
        glob.glob(
            "/opt/carla-simulator/PythonAPI/carla/dist/carla-*%d.%d-%s.egg"
            % (
                sys.version_info.major,
                sys.version_info.minor,
                "win-amd64" if os.name == "nt" else "linux-x86_64",
            )
        )[0]
    )
except IndexError:
    pass
import carla
import random
import time
import numpy as np
import rospy
from yonoarc_utils.image import to_ndarray, from_ndarray
from yonoarc_utils.header import set_timestamp, get_timestamp
from std_msgs.msg import Header
from sensor_msgs.point_cloud2 import create_cloud
from sensor_msgs.msg import PointField, NavSatFix, Imu, CameraInfo
from carla_msgs.msg import (
    CarlaRadarMeasurement,
    CarlaRadarDetection,
    CarlaCollisionEvent,
    CarlaLaneInvasionEvent,
)
import transforms as trans


class CARLABlock:
    def on_start(self):
        """This function is called when the block starts
        we create a carla user and retrive some block's properties to configure the simulation
        like towns, weather, ego vehicle ... etc.
        """
        # username and password for ssh
        os.environ["USERNAME"] = "carla"
        os.environ["PASSWORD"] = "carla"
        # Run the ssh-server
        os.system(
            "(mkdir /home/$USERNAME && useradd $USERNAME && echo $USERNAME:$PASSWORD | chpasswd && usermod -aG sudo $USERNAME && chown carla:carla /home/carla && usermod -d /home/carla carla && mkdir /var/run/sshd && /usr/sbin/sshd) & (rm -f /tmp/.X1-lock; sh /usr/local/bin/start_desktop.sh) &"
        )
        # Let's get the Block properties and configure the the simulation
        self.carla_towns = [
            "Town01",
            "Town02",
            "Town03",
            "Town04",
            "Town05",
            "Town06",
            "Town07",
            "Town10HD",
        ]
        self.selected_town = self.carla_towns[self.get_property("towns")]
        self.alert("Selected Town is {}".format(self.selected_town), "INFO")
        self.spawn_persons = int(self.get_property("spawn_persons"))
        self.spawn_vehicles = int(self.get_property("spawn_vehicles"))
        self.enable_weather = self.get_property("enable_weather")
        self.weather_r = self.get_property("weather_r")
        self.autopilot_ = self.get_property("autopilot")
        self.vehicles_ = [
            "vehicle.audi.a2",
            "vehicle.audi.etron",
            "vehicle.audi.tt",
            "vehicle.bh.crossbike",
            "vehicle.bmw.grandtourer",
            "vehicle.bmw.isetta",
            "vehicle.carlamotors.carlacola",
            "vehicle.chevrolet.impala",
            "vehicle.citroen.c3",
            "vehicle.diamondback.century",
            "vehicle.dodge_charger.police",
            "vehicle.gazelle.omafiets",
            "vehicle.harley-davidson.low_rider",
            "vehicle.jeep.wrangler_rubicon",
            "vehicle.kawasaki.ninja",
            "vehicle.lincoln.mkz2017",
            "vehicle.mercedes-benz.coupe",
            "vehicle.mini.cooperst",
            "vehicle.mustang.mustang",
            "vehicle.nissan.micra",
            "vehicle.nissan.patrol",
            "vehicle.seat.leon",
            "vehicle.tesla.cybertruck",
            "vehicle.tesla.model3",
            "vehicle.toyota.prius",
            "vehicle.volkswagen.t2",
            "vehicle.yamaha.yzf",
        ]
        self.ego_vehicle = self.vehicles_[self.get_property("ego_vehicle")]
        self.weather_presets = [
            carla.WeatherParameters.ClearNoon,
            carla.WeatherParameters.CloudyNoon,
            carla.WeatherParameters.WetNoon,
            carla.WeatherParameters.WetCloudyNoon,
            carla.WeatherParameters.SoftRainNoon,
            carla.WeatherParameters.MidRainyNoon,
            carla.WeatherParameters.HardRainNoon,
            carla.WeatherParameters.ClearSunset,
            carla.WeatherParameters.CloudySunset,
            carla.WeatherParameters.WetSunset,
            carla.WeatherParameters.WetCloudySunset,
            carla.WeatherParameters.SoftRainSunset,
            carla.WeatherParameters.MidRainSunset,
            carla.WeatherParameters.HardRainSunset,
        ]
        self.quality_level_ = ["Epic", "Low"]
        self.selected_quality_ = self.quality_level_[self.get_property("quality_level")]
        self.alert("Quality Level is: {}".format(self.selected_quality_), "INFO")
        self._enable_lidar = self.get_property("e_lidar")
        self._enable_rgb_cam = self.get_property("e_rgb_cam")
        self._enable_gnss = self.get_property("e_gnss")
        self._enable_semantic = self.get_property("e_semantic")
        self._enable_imu = self.get_property("e_imu")
        self._enable_radar = self.get_property("e_radar")
        self._enable_collision = self.get_property("e_collision")
        self._enable_lanes_inv = self.get_property("e_lanes_inv")
        self._lidar_frame = self.get_property("lidar_frame")
        self._imu_frame = self.get_property("imu_frame")
        self._gnss_frame = self.get_property("gnss_frame")
        self._radar_frame = self.get_property("radar_frame")
        time.sleep(5)
        # Now we will run carla as a server by the user carla, as carla only runs by non-root user
        # we will use opengl not vulkan for headless mode an pass the selected town as an argument to carla
        self.pro = subprocess.Popen(
            "runuser -l carla -c 'SDL_VIDEODRIVER=offscreen sh /opt/carla-simulator/bin/CarlaUE4.sh {} -quality-level={} -opengl -carla-server'".format(
                self.selected_town, self.selected_quality_
            ),
            shell=True,
        )
        self.alert("Starting CARLA", "INFO")
        time.sleep(10)  # give CARLA server time to start

    def run(self):
        """This function will be called after on_start returns, all the magic happens here, we will create Carla client
        and add all the sensors, and actors we need
        """
        actor_list = []
        client = carla.Client("localhost", 2000)
        client.set_timeout(2.0)
        while True:
            try:
                # Once we have a client we can retrieve the world that is currently
                # running.
                world = client.get_world()
                break
            except Exception as e:
                print(e)
                print("client not connected yet")
                time.sleep(1)

        # The world contains the list blueprints that we can use for adding new
        # actors into the simulation.
        blueprint_library = world.get_blueprint_library()

        # Now let's filter all the blueprints of type 'vehicle' and choose one
        # at random.
        # random.choice(blueprint_library.filter('vehicle'))
        bp = blueprint_library.find(self.ego_vehicle)

        # A blueprint contains the list of attributes that define a vehicle's
        # instance, we can read them and modify some of them. For instance,
        # let's randomize its color.
        if bp.has_attribute("color"):
            color = random.choice(bp.get_attribute("color").recommended_values)
            bp.set_attribute("color", color)

        # Now we need to give an initial transform to the vehicle. We choose a
        # random transform from the list of recommended spawn points of the map.
        transform = random.choice(world.get_map().get_spawn_points())

        # So let's tell the world to spawn the vehicle.
        self.vehicle = world.spawn_actor(bp, transform)

        # It is important to note that the actors we create won't be destroyed
        # unless we call their "destroy" function. If we fail to call "destroy"
        # they will stay in the simulation even after we quit the Python script.
        # For that reason, we are storing all the actors we create so we can
        # destroy them afterwards.
        actor_list.append(self.vehicle)
        self.alert("Ego vehicle spawned", "INFO")
        if self.autopilot_:
            # Let's put the vehicle to drive around.
            self.vehicle.set_autopilot(True)

        # Create Bird-Eye view camera of the simulation
        camera_bp = blueprint_library.find("sensor.camera.rgb")
        camera_transform_bird_eye = carla.Transform(
            carla.Location(x=0, z=10), carla.Rotation(pitch=-90)
        )
        camera_bird_eye = world.spawn_actor(
            camera_bp, camera_transform_bird_eye, attach_to=self.vehicle
        )
        actor_list.append(camera_bird_eye)
        camera_bird_eye.listen(self.__bird_eye_image)

        if self._enable_rgb_cam:
            # Let's add now a "rgb" camera attached to the vehicle. Note that the
            # transform we give here is now relative to the vehicle.
            camera_transform = carla.Transform(carla.Location(x=1.5, z=2.4))
            camera = world.spawn_actor(
                camera_bp, camera_transform, attach_to=self.vehicle
            )
            actor_list.append(camera)
            self.alert("RGB camera sensor Activated", "INFO")
            camera.listen(self._publish_bgr_image)
            self._build_camera_info(camera)

        if self._enable_semantic:
            # Let's add now a "semantic" camera attached to the vehicle. Note that the
            # transform we give here is now relative to the vehicle.
            camera_ss_bp = blueprint_library.find("sensor.camera.semantic_segmentation")
            camera_ss = world.spawn_actor(
                camera_ss_bp, camera_transform, attach_to=self.vehicle
            )
            actor_list.append(camera_ss)
            self.alert("Semantic Segmentation Activated", "INFO")
            camera_ss.listen(self._publish_semantic_seg)

        if self._enable_lidar:
            # Let's add now a "lidar" attached to the vehicle. Note that the
            # transform we give here is now relative to the vehicle.
            lidar_bp = blueprint_library.find("sensor.lidar.ray_cast")
            lidar_bp.set_attribute("range", "50")
            lidar_bp.set_attribute("points_per_second", "320000")
            lidar_bp.set_attribute("upper_fov", "2.0")
            lidar_bp.set_attribute("lower_fov", "-26.8")
            lidar_bp.set_attribute("rotation_frequency", "20")
            lidar_transform = carla.Transform(carla.Location(x=0, z=2.4))
            lidar = world.spawn_actor(lidar_bp, lidar_transform, attach_to=self.vehicle)
            actor_list.append(lidar)
            self.alert("Lidar Sensor Activated", "INFO")
            lidar.listen(self._publish_pointcloud)

        if self._enable_gnss:
            gnss_bp = blueprint_library.find("sensor.other.gnss")
            gnss_transform = carla.Transform(carla.Location(x=0, z=2.4))
            gnss = world.spawn_actor(gnss_bp, gnss_transform, attach_to=self.vehicle)
            actor_list.append(gnss)
            self.alert("GNSS Sensor Activated", "INFO")
            gnss.listen(self._publish_gnss)

        if self._enable_radar:
            radar_bp = blueprint_library.find("sensor.other.radar")
            radar_transform = carla.Transform(carla.Location(x=2, z=1.5))
            radar_bp.set_attribute("range", "100")
            radar_bp.set_attribute("points_per_second", "1500")
            radar_bp.set_attribute("horizontal_fov", "30.0")
            radar_bp.set_attribute("vertical_fov", "10.0")
            radar = world.spawn_actor(radar_bp, radar_transform, attach_to=self.vehicle)
            actor_list.append(radar)
            self.alert("Radar Sensor Activated", "INFO")
            radar.listen(self._publish_radar)

        if self._enable_imu:
            imu_bp = blueprint_library.find("sensor.other.imu")
            imu_transform = carla.Transform(carla.Location(x=2, z=2))
            imu = world.spawn_actor(imu_bp, imu_transform, attach_to=self.vehicle)
            actor_list.append(imu)
            self.alert("IMU Sensor Activated", "INFO")
            imu.listen(self._publish_imu)

        if self._enable_collision:
            collision_bp = blueprint_library.find("sensor.other.collision")
            collision_transform = carla.Transform(carla.Location(x=0, z=0))
            collision = world.spawn_actor(
                collision_bp, collision_transform, attach_to=self.vehicle
            )
            actor_list.append(collision)
            self.alert("Collision Sensor Activated", "INFO")
            collision.listen(self._publish_collision)

        if self._enable_lanes_inv:
            lane_bp = blueprint_library.find("sensor.other.lane_invasion")
            lane_transform = carla.Transform(carla.Location(x=0, z=0))
            lane_inv = world.spawn_actor(
                lane_bp, lane_transform, attach_to=self.vehicle
            )
            actor_list.append(lane_inv)
            self.alert("Lanes invasion Sensor Activated", "INFO")
            lane_inv.listen(self._publish_lane_inv)

        # Here we will use some examples script provided by CARLA to spawn actors and change weather
        if self.spawn_persons > 0 or self.spawn_vehicles > 0:
            print("spawning vehicles and walkers")
            os.system(
                "cd /opt/carla-simulator/PythonAPI/examples && python3 spawn_npc.py -n {} -w {} &".format(
                    self.spawn_vehicles, self.spawn_persons
                )
            )
        if self.enable_weather:
            print("Enabling Dynamic weather")
            os.system(
                "cd /opt/carla-simulator/PythonAPI/examples && python3 dynamic_weather.py --speed {} &".format(
                    self.weather_r
                )
            )
        else:
            weather = self.weather_presets[self.get_property("weather_pre")]
            world.set_weather(weather)

        print("SLeeping")
        time.sleep(15000000)

    def _publish_bgr_image(self, image, port_key="rgb_camera"):
        """callback function to rgb camera sensor
        it converts the image to ROS image in BGR8 encoding
        """
        carla_image_data_array = np.ndarray(
            shape=(image.height, image.width, 4), dtype=np.uint8, buffer=image.raw_data
        )
        header = Header()
        set_timestamp(header, image.timestamp)
        img_msg = from_ndarray(carla_image_data_array[:, :, :3], header)
        self.publish(port_key + "/image_raw", img_msg)
        self._camera_info.header = header
        self.publish(port_key + "/camera_info", self._camera_info)

    def __bird_eye_image(self, image):
        carla_image_data_array = np.ndarray(
            shape=(image.height, image.width, 4), dtype=np.uint8, buffer=image.raw_data
        )
        header = Header()
        set_timestamp(header, image.timestamp)
        img_msg = from_ndarray(carla_image_data_array[:, :, :3], header)
        self.publish("bird_eye", img_msg)

    def _publish_semantic_seg(self, carla_image):
        """callback function to semantic segmentation camera sensor
        it converts the image to ROS image in BGR8 encoding
        """
        carla_image.convert(carla.ColorConverter.CityScapesPalette)
        carla_image_data_array = np.ndarray(
            shape=(carla_image.height, carla_image.width, 4),
            dtype=np.uint8,
            buffer=carla_image.raw_data,
        )
        header = Header()
        set_timestamp(header, carla_image.timestamp)
        img_msg = from_ndarray(carla_image_data_array[:, :, :3], header)
        self.publish("image_seg", img_msg)

    def _publish_pointcloud(self, carla_lidar_measurement):
        """
        Function to transform the a received lidar measurement into a ROS point cloud message
        :param carla_lidar_measurement: carla lidar measurement object
        :type carla_lidar_measurement: carla.LidarMeasurement
        """
        header = Header()
        set_timestamp(header, carla_lidar_measurement.timestamp)
        header.frame_id = self._lidar_frame
        fields = [
            PointField("x", 0, PointField.FLOAT32, 1),
            PointField("y", 4, PointField.FLOAT32, 1),
            PointField("z", 8, PointField.FLOAT32, 1),
            PointField("intensity", 12, PointField.FLOAT32, 1),
        ]

        lidar_data = np.fromstring(carla_lidar_measurement.raw_data, dtype=np.float32)
        lidar_data = np.reshape(lidar_data, (int(lidar_data.shape[0] / 4), 4))
        # we take the oposite of y axis
        # (as lidar point are express in left handed coordinate system, and ros need right handed)
        lidar_data[:, 1] *= -1
        point_cloud_msg = create_cloud(header, fields, lidar_data)
        self.publish("lidar", point_cloud_msg)

    def _publish_gnss(self, carla_gnss_measurement):
        """
        Function to transform a received gnss event into a ROS NavSatFix message
        :param carla_gnss_measurement: carla gnss measurement object
        :type carla_gnss_measurement: carla.GnssMeasurement
        """
        navsatfix_msg = NavSatFix()
        set_timestamp(navsatfix_msg.header, carla_gnss_measurement.timestamp)
        navsatfix_msg.header.frame_id = self._gnss_frame
        navsatfix_msg.latitude = carla_gnss_measurement.latitude
        navsatfix_msg.longitude = carla_gnss_measurement.longitude
        navsatfix_msg.altitude = carla_gnss_measurement.altitude
        self.publish("fix", navsatfix_msg)

    def _publish_radar(self, carla_radar_measurement):
        """
        Function to transform the a received Radar measurement into a ROS message
        :param carla_radar_measurement: carla Radar measurement object
        :type carla_radar_measurement: carla.RadarMeasurement
        """
        radar_msg = CarlaRadarMeasurement()
        set_timestamp(radar_msg.header, carla_radar_measurement.timestamp)
        radar_msg.header.frame_id = self._radar_frame
        for detection in carla_radar_measurement:
            radar_detection = CarlaRadarDetection()
            radar_detection.altitude = detection.altitude
            radar_detection.azimuth = detection.azimuth
            radar_detection.depth = detection.depth
            radar_detection.velocity = detection.velocity
            radar_msg.detections.append(radar_detection)
        self.publish("radar", radar_msg)

    def _publish_imu(self, carla_imu_measurement):
        """
        Function to transform a received imu measurement into a ROS Imu message
        :param carla_imu_measurement: carla imu measurement object
        :type carla_imu_measurement: carla.IMUMeasurement
        """
        imu_msg = Imu()
        imu_msg.header.frame_id = self._imu_frame
        set_timestamp(imu_msg.header, carla_imu_measurement.timestamp)
        # Carla uses a left-handed coordinate convention (X forward, Y right, Z up).
        # Here, these measurements are converted to the right-handed ROS convention
        #  (X forward, Y left, Z up).
        imu_msg.angular_velocity.x = -carla_imu_measurement.gyroscope.x
        imu_msg.angular_velocity.y = carla_imu_measurement.gyroscope.y
        imu_msg.angular_velocity.z = -carla_imu_measurement.gyroscope.z

        imu_msg.linear_acceleration.x = carla_imu_measurement.accelerometer.x
        imu_msg.linear_acceleration.y = -carla_imu_measurement.accelerometer.y
        imu_msg.linear_acceleration.z = carla_imu_measurement.accelerometer.z

        imu_rotation = carla_imu_measurement.transform.rotation

        quat = trans.carla_rotation_to_numpy_quaternion(imu_rotation)
        imu_msg.orientation = trans.numpy_quaternion_to_ros_quaternion(quat)
        self.publish("imu", imu_msg)

    def _publish_collision(self, collision_event):
        """
        Function to wrap the collision event into a ros messsage
        :param collision_event: carla collision event object
        :type collision_event: carla.CollisionEvent
        """
        collision_msg = CarlaCollisionEvent()
        collision_msg.header.frame_id = "ego_vehicle"
        set_timestamp(collision_msg.header, collision_event.timestamp)
        collision_msg.other_actor_id = collision_event.other_actor.id
        collision_msg.normal_impulse.x = collision_event.normal_impulse.x
        collision_msg.normal_impulse.y = collision_event.normal_impulse.y
        collision_msg.normal_impulse.z = collision_event.normal_impulse.z
        self.publish("collision", collision_msg)

    def _publish_lane_inv(self, lane_invasion_event):
        """
        Function to wrap the lane invasion event into a ros messsage
        :param lane_invasion_event: carla lane invasion event object
        :type lane_invasion_event: carla.LaneInvasionEvent
        """
        lane_invasion_msg = CarlaLaneInvasionEvent()
        lane_invasion_msg.header.frame_id = "ego_vehicle"
        set_timestamp(lane_invasion_msg.header, lane_invasion_event.timestamp)
        for marking in lane_invasion_event.crossed_lane_markings:
            lane_invasion_msg.crossed_lane_markings.append(marking.type)
        self.publish("lanes_invasion", lane_invasion_msg)

    def _build_camera_info(self, carla_actor):
        """
        Private function to compute camera info
        camera info doesn't change over time
        """
        camera_info = CameraInfo()
        # store info without header
        camera_info.header = None
        camera_info.width = int(carla_actor.attributes["image_size_x"])
        camera_info.height = int(carla_actor.attributes["image_size_y"])
        camera_info.distortion_model = "plumb_bob"
        cx = camera_info.width / 2.0
        cy = camera_info.height / 2.0
        fx = camera_info.width / (
            2.0 * math.tan(float(carla_actor.attributes["fov"]) * math.pi / 360.0)
        )
        fy = fx
        camera_info.K = [fx, 0, cx, 0, fy, cy, 0, 0, 1]
        camera_info.D = [0, 0, 0, 0, 0]
        camera_info.R = [1.0, 0, 0, 0, 1.0, 0, 0, 0, 1.0]
        camera_info.P = [fx, 0, cx, 0, 0, fy, cy, 0, 0, 0, 1.0, 0]
        self._camera_info = camera_info

    def on_new_messages(self, messages):
        """ [Optional] Called according to the execution mode of the block.

        Parameters
        ----------
        messages : dict
            A dictionary of the port keys and the values of the incoming messages.

        """
        if "control" in messages:
            ros_vehicle_control = messages["control"]
            control_cmd = self.vehicle.get_control()
            control_cmd.hand_brake = ros_vehicle_control.hand_brake
            control_cmd.brake = ros_vehicle_control.brake
            control_cmd.steer = ros_vehicle_control.steer
            control_cmd.throttle = ros_vehicle_control.throttle
            control_cmd.reverse = ros_vehicle_control.reverse
            self.vehicle.apply_control(control_cmd)
