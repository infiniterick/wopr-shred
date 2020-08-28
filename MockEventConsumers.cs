
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using MassTransit;
using ServiceStack.Redis;
using Wopr.Core;

namespace Wopr.Shred{

    public class UserUpdatedConsumerMock : IConsumer<UserUpdated> {

        IRedisClientsManager redis;
    
        public UserUpdatedConsumerMock(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<UserUpdated> context){
            Console.WriteLine(JsonSerializer.Serialize(context.Message));
            return Task.CompletedTask;
        }
    }

    public class ContentReceivedConsumerMock : IConsumer<ContentReceived> {

        IRedisClientsManager redis;
    
        public ContentReceivedConsumerMock(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<ContentReceived> context){
            Console.WriteLine(JsonSerializer.Serialize(context.Message));
            return Task.CompletedTask;
        }
    }
    
    public class ReactionAddedConsumerMock : IConsumer<ReactionAdded> {

        IRedisClientsManager redis;
    
        public ReactionAddedConsumerMock(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<ReactionAdded> context){
            Console.WriteLine(JsonSerializer.Serialize(context.Message));
            return Task.CompletedTask;
        }
    }

    public class ReactionRemovedConsumerMock : IConsumer<ReactionRemoved> {

        IRedisClientsManager redis;
    
        public ReactionRemovedConsumerMock(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<ReactionRemoved> context){
            Console.WriteLine(JsonSerializer.Serialize(context.Message));
            return Task.CompletedTask;
        }
    }

    public class ConnectedConsumerMock : IConsumer<Connected> {

        IRedisClientsManager redis;
    
        public ConnectedConsumerMock(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<Connected> context){
            Console.WriteLine(JsonSerializer.Serialize(context.Message));
            return Task.CompletedTask;;
        }
    }

    public class DisconnectedConsumerMock : IConsumer<Disconnected> {

        IRedisClientsManager redis;
    
        public DisconnectedConsumerMock(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<Disconnected> context){
            Console.WriteLine(JsonSerializer.Serialize(context.Message));
            return Task.CompletedTask;
        }
    }
  }