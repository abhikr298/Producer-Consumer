import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const stream = Kafka.Producer.createWriteStream({
  'metadata.broker.list': 'localhost:9092'
}, {}, {
  topic: 'test'
});

stream.on('error', (err) => {
  console.error('Error in our kafka stream');
  console.error(err);
});

function queueRandomMessage() {
  const category = getRandomAnimal();
  const legs = getNumberOfLegs(category);
  const event = { category, legs };
  const success = stream.write(eventType.toBuffer(event));     
  if (success) {
    console.log(`message queued (${JSON.stringify(event)})`);
  } else {
    console.log('Too many messages in the queue already..');
  }
}

function getRandomAnimal() {
  const categories = ['COW', 'HUMAN'];
  return categories[Math.floor(Math.random() * categories.length)];
}

function getNumberOfLegs(animal) {
  if (animal === 'COW') {
    return '4';
  } else if (animal === 'HUMAN') {
    return '2';
  } else {
    return 'nothing...';
  }
}

setInterval(() => {
  queueRandomMessage();
}, 3000);