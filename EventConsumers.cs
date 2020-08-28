
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using MassTransit;
using ServiceStack.Redis;
using Wopr.Core;

namespace Wopr.Shred {
  
    public class UserUpdatedConsumer : IConsumer<UserUpdated> {

        IRedisClientsManager redis;
    
        public UserUpdatedConsumer(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<UserUpdated> context){
            var userKey = $"{RedisPaths.ModelUser}:{context.Message.User.Id}";
            var userStatusKey = $"{RedisPaths.ModelUserStatus}:{context.Message.User.Id}";

            using(var innerClient = redis.GetClient())
            using(var pipeline = innerClient.CreatePipeline()){
                
                //update the user's name and status
                pipeline.QueueCommand(client => client.SetRangeInHash(userKey, new KeyValuePair<string,string>[]{
                    new KeyValuePair<string, string>("name", context.Message.User.Name),
                    new KeyValuePair<string, string>("status", context.Message.Status),
                    new KeyValuePair<string, string>("activityname", context.Message.Activity?.Name != null ? context.Message.Activity.Name : ""),
                    new KeyValuePair<string, string>("activitydetails", context.Message.Activity?.Details != null ? context.Message.Activity.Details : "")
                }));

                //add status change to users status change history set
                pipeline.QueueCommand(client => client.AddItemToSortedSet(userStatusKey, JsonSerializer.Serialize(context.Message), context.Message.Timestamp.Ticks));
                pipeline.QueueCommand(client => client.PublishMessage(userKey, "changed"));

                pipeline.Flush();
            }

            return Task.CompletedTask;
        }
    }

    public class ContentReceivedConsumer : IConsumer<ContentReceived> {

        IRedisClientsManager redis;
    
        public ContentReceivedConsumer(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<ContentReceived> context){
            var channelKey = $"{RedisPaths.ModelChannel}:{context.Message.Channel.Id}";

            //content is null on updates for some reason, ignore them to prevent nuking of original messages
            if(context.Message.Content != null){
                
                using(var innerClient = redis.GetClient())
                using(var pipeline = innerClient.CreatePipeline()){
                    
                    //update the channel name
                    pipeline.QueueCommand(client => client.SetEntryInHash(channelKey, "name", context.Message.Channel.Name));

                    //store the message text directly in the channel index leaf
                    pipeline.QueueCommand(client => client.SetEntryInHash(channelKey, context.Message.MessageId, context.Message.Content));

                    //store the whole message so consumers can read other attributes
                    pipeline.QueueCommand(client => client.SetEntryInHash(RedisPaths.ModelContent, context.Message.MessageId, JsonSerializer.Serialize(context.Message)));

                    //publish events
                    pipeline.QueueCommand(client => client.PublishMessage(channelKey, "changed"));

                    pipeline.Flush();
                }
            }

            return Task.CompletedTask;
        }
    }
    
    public class ReactionAddedConsumer : IConsumer<ReactionAdded> {

        IRedisClientsManager redis;
    
        public ReactionAddedConsumer(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<ReactionAdded> context){
            var messageKey = $"{RedisPaths.ModelReaction}:{context.Message.MessageId}";

            using(var innerClient = redis.GetClient())
            using(var pipeline = innerClient.CreatePipeline()){
                
                //update reactions table for this message
                pipeline.QueueCommand(client => client.IncrementValueInHash(messageKey, context.Message.Emote, 1));
                
                //publish events
                pipeline.QueueCommand(client => client.PublishMessage(messageKey, "changed"));

                pipeline.Flush();
            }

            return Task.CompletedTask;
        }
    }

    public class ReactionRemovedConsumer : IConsumer<ReactionRemoved> {

        IRedisClientsManager redis;
    
        public ReactionRemovedConsumer(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<ReactionRemoved> context){
            var messageKey = $"{RedisPaths.ModelReaction}:{context.Message.MessageId}";

            using(var innerClient = redis.GetClient())
            using(var pipeline = innerClient.CreatePipeline()){
                
                //update reactions table for this message
                pipeline.QueueCommand(client => client.IncrementValueInHash(messageKey, context.Message.Emote, -1));

                //publish events
                pipeline.QueueCommand(client => client.PublishMessage(messageKey, "changed"));

                pipeline.Flush();
            }

            return Task.CompletedTask;
        }
    }

    public class ConnectedConsumer : IConsumer<Connected> {

        IRedisClientsManager redis;
    
        public ConnectedConsumer(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<Connected> context){
            using(var innerClient = redis.GetClient())
            using(var pipeline = innerClient.CreatePipeline()){
                
                //update the user's name and status
                pipeline.QueueCommand(client => client.SetRangeInHash(RedisPaths.ModelServerInfo, new KeyValuePair<string,string>[]{
                    new KeyValuePair<string, string>("status", "connected"),
                    new KeyValuePair<string, string>("lastupdated", context.Message.Timestamp.ToString()),
                }));

                //publish events
                pipeline.QueueCommand(client => client.PublishMessage(RedisPaths.ModelServerInfo, "changed"));

                pipeline.Flush();
            }

            return Task.CompletedTask;
        }
    }

    public class DisconnectedConsumer : IConsumer<Disconnected> {

        IRedisClientsManager redis;
    
        public DisconnectedConsumer(IRedisClientsManager redis){
            this.redis = redis;
        }

        public Task Consume(ConsumeContext<Disconnected> context){
            using(var innerClient = redis.GetClient())
            using(var pipeline = innerClient.CreatePipeline()){
                
                //update the user's name and status
                pipeline.QueueCommand(client => client.SetRangeInHash(RedisPaths.ModelServerInfo, new KeyValuePair<string,string>[]{
                    new KeyValuePair<string, string>("status", "disconnected"),
                    new KeyValuePair<string, string>("lastupdated", context.Message.Timestamp.ToString()),
                    new KeyValuePair<string, string>("lastdisconnectreason", context.Message.ExtraInfo != null ? context.Message.ExtraInfo : "none" ),
                }));

                //publish events
                pipeline.QueueCommand(client => client.PublishMessage(RedisPaths.ModelServerInfo, "changed"));
                
                pipeline.Flush();
            }

            return Task.CompletedTask;
            
        }
    }
  }