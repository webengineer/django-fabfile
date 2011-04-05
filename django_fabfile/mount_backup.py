#!/usr/bin/env python

'''
This script mounts an AWS EBS-store server backup snapshot for inspection,
and currently covers the following two cases:

1. backups in us-east region are mounted on the existing "infrastructure" server,
   and in us-east this script should be run on the infrastructure server
2. for backups in us-west region, a temporary server is spun-up, and the backup
   is attached to that temporary server. For us-west, the script can be run anywhere
   there exists a python environment and an appropriate version of boto.

Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials
'''

import boto.ec2 # AWS API
import os   # working with local directories.
import time # need to wait for some things (time.sleep)

device = '/dev/sdm' # where the backup volume will be attached to the mountOnServer

print '\nThis script will mount a server backup in your Amazon AWS account.\n'

#################################################
# Choose your region (where the server whose backup you wish to see is running)
#################################################

region = ' '
while region == ' ':

	print "1. us-east-1"
	print "2. us-west-1"

	res = raw_input('In which region is the server / backup that you would like to look at?: ')
	res = res.strip() # remove whitspace

	if res == '1':
		region = 'us-east-1'
		mountOnServerLabel = 'infrastructure' # server the backup volume will be mounted on.
		mountOnServerID = 'i-a09a13cd'
	elif res == '2':
		region = 'us-west-1'
		mountOnServerLabel = 'temporary' # server the backup volume will be mounted on.
		mountOnServerID = 'none yet'
	else:
		print '\nIncorrect input, please try again:'

#################################################
# Authenticate to an Amazon AWS region
#################################################

conn = boto.ec2.connect_to_region(region)

#################################################
# us-west specific: identify key and create temporary server
#################################################

if region == 'us-west-1':
	
	print '\nYou have chosen ' + region + ', which has no designated server for mounting backups.'
	print 'This script will now create a temporary server for the purposes of accessing your selected backup.'
	print '(This temporary server will be deleted automatically at the end of the process.)'
	
	print '\nYou will need to have already created a login key in the ' + region + ' region.'
	print 'Here are the list of keys that currently exist in the ' + region + ' region:\n'
	
	keys = conn.get_all_key_pairs()
	for key in keys:
		print key.name
	
	success = False
	while success == False:
		res = raw_input('\nPaste your key (from the list above) here: ')
		res = res.strip() # remove whitspace
		for key in keys:
			if res == key.name:
				yourkey = key
				success = True

	print '\nYou have selected key: ' + yourkey.name
	print 'We will now start a temporary server in ' + region + ' using this key....'
	
	# This image ID is taken from http://uec-images.ubuntu.com/maverick/current/
	# It probably should be updated with something newer once a year or so.
	imageID = 'ami-d194c494'
	images = conn.get_all_images(image_ids=imageID)
	
	for image in images:
		if image.id == imageID:
			selectedImage = image

	try:
		print 'We will use Ubuntu AMI ' + selectedImage.id + ' for this server....'
	except:
		print '\nSorry, could not find the specified image ' + imageID + ' in the list of available images!'
		raise
	
	reservation = selectedImage.run(
		key_name=yourkey.name,
		instance_type='t1.micro'
	)
	
	mountOnServerID = reservation.instances[0].id
	mountOnServer = reservation.instances[0]
	
	state = ' '
	while state != 'running': # wait for the server to finish starting up
		time.sleep(4)
		mountOnServer.update()
		state = mountOnServer.state
		print 'Waiting: ' + mountOnServerLabel + ' server state is still ' + state

#################################################
# Get all available instances in the chosen region
#################################################

print '\nFirst get all instances (servers) running in this region:\n'

instances = []
for r in conn.get_all_instances():
	instances.extend(r.instances)

print "Volume ID\tInstance ID\tState\tInstance Type\tKey Name\tLocation\tIP Address\n"

found = False
for item in instances:
	
	try:
		volumeID = item.block_device_mapping['/dev/sda1'].volume_id
	except:
		volumeID = 'None'
	
	print '%s\t%s\t%s\t%s\t%s\t%s\t%s' % \
	(volumeID,  item.id, item.state,\
	 item.instance_type, item.key_name, item.placement, item.ip_address)
	
	if item.id == mountOnServerID:
		mountOnServer = item
		found = True

if found == False:
	print '\nI cannot find the ' + mountOnServerLabel + ' server in this list, exiting.....'
else:

	print '\nYour backup will be mounted on the ' + mountOnServerLabel + \
		  ' server, instance ID ' + mountOnServerID

	snapshots = conn.get_all_snapshots(owner='self')

	res = ' '
	while res != 'yes' and res != 'no':
		res = raw_input('\nDo you know exactly which volume snapshot you would like to mount (yes or no)?: ')
		res = res.strip() # remove whitspace
	
	#################################################
	# snapshot / backup ID is already known:
	#################################################

	if res == 'yes': # let the user enter a snapshot ID directly....
		
		correct = ' '
		while correct != 'y':
		
			matched = ' '
			while matched != 'yes':
		
				print ''
				prompt = 'Enter the snapshot ID that you wish to mount here: '
				chosenSnapshotID = raw_input(prompt)
				chosenSnapshotID = chosenSnapshotID.strip() # remove whitspace
		
				for item in snapshots:
					if chosenSnapshotID == item.id:
						chosenSnapshot = item
						matched = 'yes'
		
				if matched != 'yes':
					print 'Sorry, cannot find your snapshot in the list. Please try again.'
		
			print '\nYou have chosen:'
			print '%s\t%s\t%s\t%s' % \
				(chosenSnapshot.id, chosenSnapshot.volume_id, \
				 chosenSnapshot.start_time, chosenSnapshot.description)
	
			correct = raw_input('Is this correct (y or n)?: ')
			correct = correct.strip() # remove whitspace
		
	#################################################
	# Snapshot ID is not known in advance, select a server:
	#################################################

	else: # prompt the user to select server, then show him the snapshots for that server....

		chosenInstance = ' '
		while chosenInstance == ' ':
	
			print ''
			chosenVolumeID = raw_input('From the above list, paste the volume ID of the instance whose backups you wish to access: ')
			chosenVolumeID = chosenVolumeID.strip() # remove whitspace
	
			for item in instances:
				
				try: # some instances do not have volumes, we only care about those that do
					if chosenVolumeID == item.block_device_mapping['/dev/sda1'].volume_id:
						chosenInstance = item
				except:
					continue
			
			if chosenInstance == ' ':
				print '\nThe volume ID you entered does match any of the available instances.\nPlease try again:'
	
		print '\nThis is the instance you have chosen:'
		print '%s\t%s\t%s\t%s\t%s\t%s' % \
			(chosenInstance.block_device_mapping['/dev/sda1'].volume_id, chosenInstance.id, \
			 chosenInstance.state, chosenInstance.instance_type, \
			 chosenInstance.key_name, chosenInstance.ip_address)
	
		#################################################
		# List the volume snapshots that match the chosen Volume ID
		#################################################
	
		print "\nSnapshot ID\tVolume ID\tDate\t\t\t\tDescription\n"
	
		for item in snapshots:
			if item.volume_id == chosenVolumeID:
				defaultSnapshot = item # AWS by default lists from oldest to newest, so the last one will be newest
				print '%s\t%s\t%s\t%s' % \
				(item.id, item.volume_id, item.start_time, item.description)
	
		#################################################
		# Choose which snapshot to mount
		#################################################
	
		correct = 'n'
		matched = ' '
		while correct != 'y':
	
			print ''
			prompt = 'Paste the snapshot ID that you wish to mount (default = %s): ' % defaultSnapshot.id
			chosenSnapshotID = raw_input(prompt)
			chosenSnapshotID = chosenSnapshotID.strip() # remove whitspace
	
			for item in snapshots:
				if chosenSnapshotID == item.id:
					chosenSnapshot = item
					matched = 'yes'
	
			if matched != 'yes':
				chosenSnapshot = defaultSnapshot
	
			print '\nYou have chosen:'
			print '%s\t%s\t%s\t%s' % \
				(chosenSnapshot.id, chosenSnapshot.volume_id, \
				 chosenSnapshot.start_time, chosenSnapshot.description)
	
			correct = raw_input('Is this correct (y or n)?: ')
			correct = correct.strip() # remove whitspace

	#################################################
	# Create a volume from the selected snapshot and attach it to the designated server
	#################################################

	print '\nCreating a new volume from snapshot ' + chosenSnapshot.id + '....'

	volume = conn.create_volume(
		size=chosenSnapshot.volume_size,
		zone=mountOnServer.placement,
		snapshot=chosenSnapshot.id
	)

	print 'New volume ID is: ' + volume.id
	print 'Attaching the new volume to the ' + mountOnServerLabel + ' server....'

	attach = volume.attach(mountOnServer.id, device)
	
	status = ' '
	while status != 'attached':
		time.sleep(2) # wait for the device to appear on the server
		volume.update()
		status = volume.attach_data.status
		print 'Waiting: volume status is still ' + status
	
	print 'Volume is attached to ' + mountOnServer.id + ' on ' + device

	if region == 'us-east-1': # mount the volume on the server

		print '\nMounting volume %s:' % volume.id

		directory = 'tmpSnapshot'
		mountpoint = '/media/' + directory
	
		print 'Creating mountpoint at ' + mountpoint
		command = 'mkdir ' + mountpoint
		os.system(command)
	
		command = 'mount -t ext3 ' + device + ' ' + mountpoint
		os.system(command)
		print 'Your backup is mounted on the ' + mountOnServerLabel + ' server at ' + \
			mountpoint + '\n'

	else: # we just created the server, user has to login and mount manually
	
		print '\nYou may now SSH into the ' + mountOnServerLabel + ' server, using:'
		print 'ssh -i /path/to/your/key/ ubuntu@' + mountOnServer.public_dns_name
		print 'then mount the backup volume located at ' + device

	#################################################
	# Cleanup processing: detach and delete backup volume, terminate temporary server
	#################################################

	finished = ' '
	while finished != 'FINISHED':
		finished = raw_input('Enter FINISHED if you are finished looking at the backup and would like to cleanup: ')
		finished = finished.strip() # remove whitspace

	print '\nThe cleanup process will almost certainly break if you have not unmounted the backup.'
	res = ' '
	while res != 'y':
		res = raw_input('Have you unmounted the backup volume (y or no)?: ')
		res = res.strip() # remove whitspace

	if region == 'us-east-1': # where we are running the script on the infrastructure server
		
		print '\nUnmounting the backup....'
		command = 'umount ' + mountpoint
		os.system(command)
		
		print 'Deleting the mountpoint....'
		command = 'rmdir ' + mountpoint
		os.system(command)
	
	print 'Detaching the volume from ' + mountOnServerLabel + ' server....'
	detach = volume.detach()

	waitStep = 2
	totalWait = 0
	status = ' '
	while status != 'available':
		time.sleep(waitStep) # wait for the volume to fully detach
		totalWait = totalWait + waitStep
		volume.update()
		status = volume.status
		print 'Waiting: volume status is still ' + status
		if totalWait > 60: # something wrong if one minute is not enough
			print 'Failed to detach volume ' + volume.id
			break

	print 'Deleting the backup volume....'
	try:
		delete = volume.delete()
	except:
		print 'Failed to delete volume ' + volume.id
	
	if mountOnServerLabel == 'temporary': # only terminate temporary server
		print 'Deleting the ' + mountOnServerLabel + ' server....'
		try:
			mountOnServer.terminate()
		except:
			print 'Failed to terminate ' + mountOnServerLabel + ' server ' + mountOnServer.id

	print '\nCleanup complete, exiting script.'
