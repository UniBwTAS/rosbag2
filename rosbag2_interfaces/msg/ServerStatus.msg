bool success                                        # flag whether previous open bag request was successful
string error_message                                # error message when not successful
builtin_interfaces/Time start                       # start time of bag
builtin_interfaces/Time end                         # end time of bag
string[] topic_names                                # included topic names
string[] topic_types                                # corresponding topic types
# TODO: Recorded QoS Profiles                       # corresponding QoS profiles
rosbag2_interfaces/StorageOptions storage_options   # previously set storage options
rosbag2_interfaces/PlayOptions play_options         # previously set play options