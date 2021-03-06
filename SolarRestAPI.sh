#!/bin/bash
 # /etc/init.d/mqtt-stage
 # version 0.3.9 2014-10-25 (YYYY-MM-DD)
 ### BEGIN INIT INFO
 # Provides:   nodered
 # Required-Start: $local_fs $remote_fs screen-cleanup
 # Required-Stop:  $local_fs $remote_fs
 # Should-Start:   $network
 # Should-Stop:    $network
 # Default-Start:  2 3 4 5
 # Default-Stop:   0 1 6
 # Short-Description:    Start mqtt-stage
 # Description:    Starts the mqtt-stage server
 ### END INIT INFO
 
 #Settings
 SERVICE='SolarDataRESTapi.py'
 OPTIONS=''
 USERNAME='iot'
 APP_PATH='/home/iot/repos/SolarDataRESTfulAPI'
 HISTORY=1024
 INVOCATION="python $SERVICE" 
 SCREEN_SESSION='SolarRestApi'
 ME=`whoami`

 as_user() {
   if [ "$ME" = "$USERNAME" ] ; then
     echo "Running: bash -c $1"
     bash -c "$1"
   else
     echo  "Running: su - $USERNAME -c \"$1\""
     su - $USERNAME -c "$1"
   fi
 }
 
 my_start() {
   if  pgrep -u $USERNAME -f $SERVICE > /dev/null
   then
     echo "$SERVICE is already running!"
   else
     echo "Starting $SERVICE..."
     cd $APP_PATH

     CMD="cd $APP_PATH && screen -h $HISTORY -dmS $SCREEN_SESSION $INVOCATION"
 
     as_user "$CMD"
     #as_user "cd $APP_PATH && screen -h $HISTORY -dmS $SCREEN_SESSION $INVOCATION"
     sleep 7
     if pgrep -u $USERNAME -f $SERVICE > /dev/null
     then
       echo "$SERVICE is now running."
     else
       echo "Error! Could not start $SERVICE!"
     fi
   fi
 }
 
 
 my_stop() {
   if pgrep -u $USERNAME -f $SERVICE > /dev/null
   then
     echo "Stopping $SERVICE"
     #as_user "screen -p 0 -S nodered -X eval 'stuff \"say SERVER SHUTTING DOWN IN 10 SECONDS. Saving map...\"\015'"
     #as_user "screen -p 0 -S nodered -X eval 'stuff \"save-all\"\015'"
     ##sleep 10
     ##as_user "screen -p 0 -S minecraft -X eval 'stuff \"stop\"\015'"
     ##sleep 7
     pkill -u iot -f $SERVICE
     sleep 7
   else
     echo "$SERVICE was not running."
   fi
   if pgrep -u $USERNAME -f $SERVICE > /dev/null
   then
     echo "Error! $SERVICE could not be stopped."
   else
     echo "$SERVICE is stopped."
   fi
 } 
 


 
 #Start-Stop here
 case "$1" in
   start)
     my_start
     ;;
   stop)
     my_stop
     ;;
   restart)
     my_stop
     my_start
     ;;
   status)
     if pgrep -u $USERNAME -f $SERVICE > /dev/null
     then
       echo "$SERVICE is running."
     else
       echo "$SERVICE is not running."
     fi
     ;;
 
   *)
   echo "Usage: $0 {start|stop|update|backup|status|restart|command \"server command\"}"
   exit 1
   ;;
 esac
 
 exit 0
 
