import subprocess
import os
import time
import glob
import sys
try:
    sys.path.append(glob.glob('/opt/carla-simulator/PythonAPI/carla/dist/carla-*%d.%d-%s.egg' % (
        sys.version_info.major,
        sys.version_info.minor,
        'win-amd64' if os.name == 'nt' else 'linux-x86_64'))[0])
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
from sensor_msgs.msg import PointField
from sensor_msgs.msg import NavSatFix


class CARLABlock:
    def on_start(self):
        """This function is called when the block starts
        we create a carla user and retrive some block's properties to configure the simulation
        like towns, weather, ego vehicle ... etc.
        """
        self.alert("Creating carla user and running ssh.....", "INFO")
        # username and password for ssh
        os.environ["USERNAME"] = "carla"
        os.environ["PASSWORD"] = "carla"
        # Run the ssh-server
        os.system("(mkdir /home/$USERNAME && useradd $USERNAME && echo $USERNAME:$PASSWORD | chpasswd && usermod -aG sudo $USERNAME && chown carla:carla /home/carla && usermod -d /home/carla carla && mkdir /var/run/sshd && /usr/sbin/sshd) & (rm -f /tmp/.X1-lock; sh /usr/local/bin/start_desktop.sh) &")
        # Let's get the Block properties and configure the the simulation
        self.carla_towns = ['Town01', 'Town02', 'Town03', 'Town04',
                            'Town05', 'Town06', 'Town07', 'Town10HD']
        self.selected_town = self.carla_towns[self.get_property('towns')]
        self.alert('Selected Town is {}'.format(self.selected_town), "INFO")
        self.spawn_persons = int(self.get_property('spawn_persons'))
        self.spawn_vehicles = int(self.get_property('spawn_vehicles'))
        self.enable_weather = self.get_property('enable_weather')
        self.weather_r = self.get_property('weather_r')
        self.autopilot_ = self.get_property('autopilot')
        self.vehicles_ = ['vehicle.audi.a2', 'vehicle.audi.etron', 'vehicle.audi.tt', 'vehicle.bh.crossbike', 'vehicle.bmw.grandtourer', 'vehicle.bmw.isetta', 'vehicle.carlamotors.carlacola', 'vehicle.chevrolet.impala', 'vehicle.citroen.c3', 'vehicle.diamondback.century', 'vehicle.dodge_charger.police', 'vehicle.gazelle.omafiets', 'vehicle.harley-davidson.low_rider',
                          'vehicle.jeep.wrangler_rubicon', 'vehicle.kawasaki.ninja', 'vehicle.lincoln.mkz2017', 'vehicle.mercedes-benz.coupe', 'vehicle.mini.cooperst', 'vehicle.mustang.mustang', 'vehicle.nissan.micra', 'vehicle.nissan.patrol', 'vehicle.seat.leon', 'vehicle.tesla.cybertruck', 'vehicle.tesla.model3', 'vehicle.toyota.prius', 'vehicle.volkswagen.t2', 'vehicle.yamaha.yzf']
        self.ego_vehicle = self.vehicles_[self.get_property('ego_vehicle')]
        self.weather_presets = [carla.WeatherParameters.ClearNoon, carla.WeatherParameters.CloudyNoon, carla.WeatherParameters.WetNoon, carla.WeatherParameters.WetCloudyNoon, carla.WeatherParameters.SoftRainNoon, carla.WeatherParameters.MidRainyNoon, carla.WeatherParameters.HardRainNoon,
                                carla.WeatherParameters.ClearSunset, carla.WeatherParameters.CloudySunset, carla.WeatherParameters.WetSunset, carla.WeatherParameters.WetCloudySunset, carla.WeatherParameters.SoftRainSunset, carla.WeatherParameters.MidRainSunset, carla.WeatherParameters.HardRainSunset]
        self.quality_level_ = ['Epic', 'Low']
        self.selected_quality_ = self.quality_level_[
            self.get_property('quality_level')]
        self.alert("Quality Level is: {}".format(
            self.selected_quality_), "INFO")
        # Now we will run carla as a server by the user carla, as carla only runs by non-root user
        # we will use opengl not vulkan for headless mode an pass the selected town as an argument to carla
        time.sleep(5)
        self.pro = subprocess.Popen(
            "runuser -l carla -c 'SDL_VIDEODRIVER=offscreen sh /opt/carla-simulator/bin/CarlaUE4.sh {} -quality-level={} -opengl -carla-server'".format(self.selected_town, self.selected_quality_), shell=True)
        self.alert("Starting CARLA", "INFO")
        time.sleep(10)  # give CARLA server time to start

    def run(self):
        """This function will be called after on_start returns, all the magic happens here, we will create Carla client
        and add all the sensors, and actors we need
        """
        actor_list = []
        client = carla.Client('localhost', 2000)
        client.set_timeout(2.0)
        while(True):
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
        if bp.has_attribute('color'):
            color = random.choice(bp.get_attribute('color').recommended_values)
            bp.set_attribute('color', color)

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
        print('created %s' % self.vehicle.type_id)
        if self.autopilot_:
            # Let's put the vehicle to drive around.
            self.vehicle.set_autopilot(True)

        # Let's add now a "rgb" camera attached to the vehicle. Note that the
        # transform we give here is now relative to the vehicle.
        camera_bp = blueprint_library.find('sensor.camera.rgb')
        camera_transform = carla.Transform(
            carla.Location(x=1.5, z=2.4))
        camera = world.spawn_actor(
            camera_bp, camera_transform, attach_to=self.vehicle)
        actor_list.append(camera)
        print('created %s' % camera.type_id)
        camera.listen(self._publish_bgr_image)
        # Create Bird-Eye view camera of the simulation
        camera_transform_bird_eye = carla.Transform(
            carla.Location(x=0, z=10), carla.Rotation(pitch=-90))
        camera_bird_eye = world.spawn_actor(
            camera_bp, camera_transform_bird_eye, attach_to=self.vehicle)
        actor_list.append(camera_bird_eye)
        print('created %s' % camera_bird_eye.type_id)
        camera_bird_eye.listen(self.__bird_eye_image)
        # Let's add now a "semantic" camera attached to the vehicle. Note that the
        # transform we give here is now relative to the vehicle.
        camera_ss_bp = blueprint_library.find(
            'sensor.camera.semantic_segmentation')
        camera_ss = world.spawn_actor(
            camera_ss_bp, camera_transform, attach_to=self.vehicle)
        actor_list.append(camera_ss)
        print('created %s' % camera_ss.type_id)
        camera_ss.listen(self._publish_semantic_seg)

        # Let's add now a "lidar" attached to the vehicle. Note that the
        # transform we give here is now relative to the vehicle.
        lidar_bp = blueprint_library.find('sensor.lidar.ray_cast')
        lidar_bp.set_attribute('range', '50')
        lidar_bp.set_attribute('points_per_second', '320000')
        lidar_bp.set_attribute("upper_fov", "2.0")
        lidar_bp.set_attribute("lower_fov", "-26.8")
        lidar_bp.set_attribute("rotation_frequency", "20")
        lidar_transform = carla.Transform(carla.Location(x=0, z=2.4))
        lidar = world.spawn_actor(
            lidar_bp, lidar_transform, attach_to=self.vehicle)
        actor_list.append(lidar)
        print('created %s' % lidar.type_id)
        lidar.listen(self._publish_pointcloud)

        # Let's add now a "gnss" attached to the vehicle. Note that the
        # transform we give here is now relative to the vehicle.
        gnss_bp = blueprint_library.find('sensor.other.gnss')
        gnss_transform = carla.Transform(carla.Location(x=0, z=2.4))
        gnss = world.spawn_actor(
            gnss_bp, gnss_transform, attach_to=self.vehicle)
        actor_list.append(gnss)
        print('created %s' % gnss.type_id)
        gnss.listen(self._publish_gnss)

        # Here we will use some examples script provided by CARLA to spawn actors and change weather
        if(self.spawn_persons > 0 or self.spawn_vehicles > 0):
            print("spawning vehicles and walkers")
            os.system(
                "cd /opt/carla-simulator/PythonAPI/examples && python3 spawn_npc.py -n {} -w {} &".format(self.spawn_vehicles, self.spawn_persons))
        if self.enable_weather:
            print("Enabling Dynamic weather")
            os.system(
                "cd /opt/carla-simulator/PythonAPI/examples && python3 dynamic_weather.py --speed {} &".format(self.weather_r))
        else:
            weather = self.weather_presets[self.get_property('weather_pre')]
            world.set_weather(weather)

        print("SLeeping")
        time.sleep(15000000)

    def _publish_bgr_image(self, image, port_key="image"):
        """callback function to rgb camera sensor
        it converts the image to ROS image in BGR8 encoding
        """
        carla_image_data_array = np.ndarray(
            shape=(image.height, image.width, 4),
            dtype=np.uint8, buffer=image.raw_data)
        header = Header()
        set_timestamp(header, image.timestamp)
        img_msg = from_ndarray(carla_image_data_array[:, :, :3], header)
        self.publish(port_key, img_msg)

    def __bird_eye_image(self, carla_image):
        self._publish_bgr_image(carla_image, port_key="bird_eye")

    def _publish_semantic_seg(self, carla_image):
        """callback function to semantic segmentation camera sensor
        it converts the image to ROS image in BGR8 encoding
        """
        carla_image.convert(carla.ColorConverter.CityScapesPalette)
        carla_image_data_array = np.ndarray(
            shape=(carla_image.height, carla_image.width, 4),
            dtype=np.uint8, buffer=carla_image.raw_data)
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
        header.frame_id = "lidar"
        fields = [
            PointField('x', 0, PointField.FLOAT32, 1),
            PointField('y', 4, PointField.FLOAT32, 1),
            PointField('z', 8, PointField.FLOAT32, 1),
        ]

        lidar_data = np.frombuffer(
            carla_lidar_measurement.raw_data, dtype=np.float32).reshape([-1, 3]).copy()  # Original data are read only
        # we take the opposite of y axis
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
        navsatfix_msg.latitude = carla_gnss_measurement.latitude
        navsatfix_msg.longitude = carla_gnss_measurement.longitude
        navsatfix_msg.altitude = carla_gnss_measurement.altitude
        self.publish("fix", navsatfix_msg)

    def on_new_messages(self, messages):
        ''' [Optional] Called according to the execution mode of the block.

        Parameters
        ----------
        messages : dict
            A dictionary of the port keys and the values of the incoming messages.

        '''
        if 'control' in messages:
            ros_vehicle_control = messages['control']
            control_cmd = self.vehicle.get_control()
            control_cmd.hand_brake = ros_vehicle_control.hand_brake
            control_cmd.brake = ros_vehicle_control.brake
            control_cmd.steer = ros_vehicle_control.steer
            control_cmd.throttle = ros_vehicle_control.throttle
            control_cmd.reverse = ros_vehicle_control.reverse
            self.vehicle.apply_control(control_cmd)
