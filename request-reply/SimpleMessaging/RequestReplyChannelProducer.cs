﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace SimpleMessaging
{
    public class RequestReplyChannelProducer<T, TResponse> : IDisposable where T : IAmAMessage where TResponse : class, IAmAResponse
    {
        private readonly Func<T, string> _messageSerializer;
        private readonly Func<string, TResponse> _messageDeserializer;
        private readonly string _routingKey;
        private const string ExchangeName = "practical-messaging-request-reply";
        private const string InvalidMessageExchangeName = "practical-messaging-invalid";
        private readonly IConnection _connection;
        private readonly IModel _channel;

        /// <summary>
        /// Create a new channel for sending point-to-point messages
        /// Under RMQ we:
        ///     1. Create a socket connection to the broker
        ///     2. Create a channel on that socket
        ///     3. Create a direct exchange on the server for point-to-point messaging
        /// We don't create the receiving queue - each consumer does that, and will route to our
        /// key.
        /// We have split producer and consumer, as they need seperate serialization/de-serialization of the message
        /// We are disposable so that we can be used within a using statement; connections
        /// are unmanaged resources and we want to remember to close them.
        /// We inject the serializer to use with this type, so we can read and write the type to the body
        /// We are following an RAI pattern here: Resource Acquisition is Initialization
        /// </summary>
        /// <param name="messageSerializer">Needs to take a message of type T and convert to a string</param>
        /// <param name="messageDeserializer">Needs to take an on the wire message body and convert to TResponse</param>
        /// <param name="hostName">localhost if not otherwise specified</param>
        public RequestReplyChannelProducer(
            Func<T, string> messageSerializer,
            Func<string, TResponse> messageDeserializer,
            string hostName = "localhost")
        {
            _messageSerializer = messageSerializer;
            _messageDeserializer = messageDeserializer;
            //just use defaults: usr: guest pwd: guest port:5672 virtual host: /
            var factory = new ConnectionFactory() { HostName = hostName };
            factory.AutomaticRecoveryEnabled = true;
            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            /* We choose to base the key off the type name, because we want tp publish to folks interested in this type
             We name the queue after that routing key as we are point-to-point and only expect one queue to receive
            eRequesthis type of message */
            _routingKey = "Request-Reply." + typeof(T).FullName;
            var queueName = _routingKey;

            var invalidRoutingKey = "invalid." + _routingKey;
            var invalidMessageQueueName = invalidRoutingKey;

            _channel.ExchangeDeclare(ExchangeName, ExchangeType.Direct, durable: true);
            var arguments = new Dictionary<string, object>()
            {
                {"x-dead-letter-exchange", InvalidMessageExchangeName},
                {"x-dead-letter-routing-key", invalidRoutingKey}
            };

            //if we are going to have persistent messages, it mostly makes sense to have a durable queue, to survive
            //restarts, or client failures
            _channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: arguments);
            _channel.QueueBind(queue: queueName, exchange: ExchangeName, routingKey: _routingKey);

            //declare a queue for invalid messages off an invalid message exchange
            //messages that we nack without requeue will go here
            _channel.ExchangeDeclare(InvalidMessageExchangeName, ExchangeType.Direct, durable: true);
            _channel.QueueDeclare(queue: invalidMessageQueueName, durable: true, exclusive: false, autoDelete: false);
            _channel.QueueBind(queue: invalidMessageQueueName, exchange: InvalidMessageExchangeName, routingKey: invalidRoutingKey);

        }

        /// <summary>
        /// Call another process and wait for the response. This blocks, as it has function call semantics
        /// We make two choices: (a) a queue per call. This has overhead but makes correlation of message
        /// between call and response trivial; (b) a queue per client, we would need to correlate responses to
        /// ensure we handled out-of-order messages (might be enough to drop ones we don't recognize). We block
        /// awaiting the response as that is an RPC semantic, over allowing a seperate consumer to receive responses
        /// and handle them via a handler. That alternative uses routing keys over queues to work and is less true RPC
        /// than request-reply
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="timeoutInMilliseconds">The time to wait for the response</param>
        public TResponse Call(T message, int timeoutInMilliseconds)
        {
            //declare a queue for replies, we can auto-delete this as it should die with us
            //auto-generate a queue name; we don't need a routing key as we just send/receive from this queue
            //Note that we do not need bind to the default exchange; any queue declared on the default exchange
            //automatically has a routing key that is the queue name. Because we choose a random
            //queue name this means we avoid any collisions

            // TODO: Declare a queue for replies, non-durable, exclusive, auto-deleting. no queue name
            var queue = _channel.QueueDeclare(durable: false, exclusive: true, autoDelete: true);

            // TODO: Assign auto generated queuename to variable for later use
            var queueName = queue.QueueName;

            // TODO: serialize the body, and turn it into a byte[] with URF8 encoding
            var body = Encoding.UTF8.GetBytes(_messageSerializer(message));

            //In order to do guaranteed delivery, we want to use the broker's message store to hold the message, 
            //so that it will be available even if the broker restarts
            // TODO: Create basic properties for the channel
            var props = _channel.CreateBasicProperties();
            // TODO: Make the message persistent
            props.Persistent = true; // props.DeliveryMode = 2;

            // TODO: Set reply to on the props to the random queue name from above
            props.ReplyTo = queueName;
            // TODO: Publish to the consumer on ExchangeName with _routingKey and props and body
            _channel.BasicPublish(ExchangeName, _routingKey, false, props, body);

            //now we want to listen
            /*
             * TODO
             * whilst a time the timeout period is not up
             *     read from the reply queue
             *     if we have a message
             *         serialize the message (hint: convert UTF8 byte array to string
             *         ack the message
             *         break
             *     else
             *         yield briefy, but not too long as we have a timeout
             * delete the reply queue when done
             * return the response
             */
            while (true)
            {
                var responseMessage = _channel.BasicGet(queueName, false);
                if (responseMessage != null)
                {
                    var textResponse = Encoding.UTF8.GetString(responseMessage.Body);
                    var response = _messageDeserializer(textResponse);
                    _channel.BasicAck(responseMessage.DeliveryTag, false);
                    _channel.QueueDelete(queueName, true, true);
                    return response;
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        ~RequestReplyChannelProducer()
        {
            ReleaseUnmanagedResources();
        }


        private void ReleaseUnmanagedResources()
        {
            _channel.Close();
            _connection.Close();
        }
    }
}