import time
import cv2
from kafka import SimpleProducer, KafkaClient

# connect to kafka
kafka = KafkaClient('localhost:9002')
producer = SimpleProducer(kafka)

topic = 'myTopic'

def video_emitter(video):
	video = cv2.VideoCapture(video)
	print "Emitting/......"

	while(video.isOpened):
		# read the image in each frame
		success, image = video.read()

		# check if file have been read till end
		if not success:
			break

		# convert the image png
		ret, jpeg = cv2.imencode('.png', image)

		# convert image to bytes and send to kafka
		producer.send_message(topic, jpeg.tobytes())

		# To reduce CPU usage create sleep time of 0.2 sec
		time.sleep(0.2)
	
	# close the capture
	video.release()
	print 'done emiiting'

if __name__ == '__main__' :
	video.emitter('video.mp4')
