using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using ServiceStack.Redis;
using Wopr.Core;

namespace Wopr.Shred
{


    public class Shredder
    {
        RedisManagerPool redisPool;
        RedisPubSubServer redisPubSub;
        CancellationToken cancel;

        public Shredder(Secrets secrets, CancellationToken cancel)
        {
            redisPool = new RedisManagerPool(secrets.RedisToken);
            redisPubSub = new RedisPubSubServer(redisPool, RedisPaths.DataReady); 
            redisPubSub.OnMessage += NewDataReady;
            this.cancel = cancel;
        }

        public void Start(){
            redisPubSub.Start();
            ShredFreshData();
        }

        public void Stop(){
            redisPubSub.Stop();
        }

     

        public void ShredFreshData(){
            var raw = string.Empty;
            
            using(var client = redisPool.GetClient()){
                do{
                    //if anything is in the processing queue, we are recovering and should resume processing on it before new data
                    raw = client.GetItemFromList(RedisPaths.DataProcessing, 0);

                    //if nothing in processing, pull a new item
                    if(string.IsNullOrEmpty(raw))
                        raw = client.BlockingPopAndPushItemBetweenLists(RedisPaths.DataFresh, RedisPaths.DataProcessing, TimeSpan.FromSeconds(5));

                    if(!string.IsNullOrEmpty(raw)){
                        
                        //if the shred worked, archive the message, otherwise put in dead letter queue
                        if(ShredRaw(raw))
                            client.PopAndPushItemBetweenLists(RedisPaths.DataProcessing, RedisPaths.DataArchive);
                        else
                            client.PopAndPushItemBetweenLists(RedisPaths.DataProcessing, RedisPaths.DataDead);

                    }
                }
                while(!cancel.IsCancellationRequested);
            }
        }

        ///Shread raw messages into the wopr object model
        //returns true if shredded without error
        //returns false if not shredded for any reason
        private bool ShredRaw(string raw){
            Console.WriteLine(raw);
            var result = false;
            if(!string.IsNullOrEmpty(raw)){
                try{
                    if(raw.StartsWith("{\"MessageType\":\"UserUpdated\"")){
                        var msg = JsonSerializer.Deserialize<UserUpdated>(raw);
                        var userKey = $"{RedisPaths.ModelUser}:{msg.User.Id}";
                        var userStatusKey = $"{RedisPaths.ModelUserStatus}:{msg.User.Id}";

                        using(var innerClient = redisPool.GetClient())
                        using(var pipeline = innerClient.CreatePipeline()){
                            
                            //update the user's name and status
                            pipeline.QueueCommand(client => client.SetRangeInHash(userKey, new KeyValuePair<string,string>[]{
                                new KeyValuePair<string, string>("name", msg.User.Name),
                                new KeyValuePair<string, string>("status", msg.Status),
                                new KeyValuePair<string, string>("activityname", msg.Activity?.Name != null ? msg.Activity.Name : ""),
                                new KeyValuePair<string, string>("activitydetails", msg.Activity?.Details != null ? msg.Activity.Details : "")
                            }));

                            //add status change to users status change history set
                            pipeline.QueueCommand(client => client.AddItemToSortedSet(userStatusKey, raw, msg.Timestamp.Ticks));
                            pipeline.QueueCommand(client => client.PublishMessage(userKey, "changed"));

                            pipeline.Flush();
                        }

                        result = true;
                    }
                    else if(raw.StartsWith("{\"MessageType\":\"ContentReceived\"") || raw.StartsWith("{\"MessageType\":\"ContentUpdated\"")){
                        var msg = JsonSerializer.Deserialize<ContentReceived>(raw);
                        var channelKey = $"{RedisPaths.ModelChannel}:{msg.Channel.Id}";

                        //content is null on updates for some reason, ignore them to prevent nuking of original messages
                        if(msg.Content != null){
                            
                            using(var innerClient = redisPool.GetClient())
                            using(var pipeline = innerClient.CreatePipeline()){
                                
                                //update the channel name
                                pipeline.QueueCommand(client => client.SetEntryInHash(channelKey, "name", msg.Channel.Name));

                                //store the message text directly in the channel index leaf
                                pipeline.QueueCommand(client => client.SetEntryInHash(channelKey, msg.MessageId, msg.Content));

                                //store the whole message so consumers can read other attributes
                                pipeline.QueueCommand(client => client.SetEntryInHash(RedisPaths.ModelContent, msg.MessageId, raw));

                                //publish events
                                pipeline.QueueCommand(client => client.PublishMessage(channelKey, "changed"));

                                pipeline.Flush();
                            }
                        }

                        result = true;
                    }
                    else if(raw.StartsWith("{\"MessageType\":\"ReactionAdded\"")){
                        var msg = JsonSerializer.Deserialize<ReactionAdded>(raw);
                        var messageKey = $"{RedisPaths.ModelReaction}:{msg.MessageId}";

                        using(var innerClient = redisPool.GetClient())
                        using(var pipeline = innerClient.CreatePipeline()){
                            
                            //update reactions table for this message
                            pipeline.QueueCommand(client => client.IncrementValueInHash(messageKey, msg.Emote, 1));
                            
                            //publish events
                            pipeline.QueueCommand(client => client.PublishMessage(messageKey, "changed"));

                            pipeline.Flush();
                        }

                        result = true;
                    }
                    else if(raw.StartsWith("{\"MessageType\":\"ReactionRemoved\"")){
                        var msg = JsonSerializer.Deserialize<ReactionRemoved>(raw);
                        var messageKey = $"{RedisPaths.ModelReaction}:{msg.MessageId}";

                        using(var innerClient = redisPool.GetClient())
                        using(var pipeline = innerClient.CreatePipeline()){
                            
                            //update reactions table for this message
                            pipeline.QueueCommand(client => client.IncrementValueInHash(messageKey, msg.Emote, -1));

                            //publish events
                            pipeline.QueueCommand(client => client.PublishMessage(messageKey, "changed"));

                            pipeline.Flush();
                        }

                        result = true;
                    }
                    else if(raw.StartsWith("{\"MessageType\":\"Connected\"")){
                        var msg = JsonSerializer.Deserialize<Connected>(raw);

                        using(var innerClient = redisPool.GetClient())
                        using(var pipeline = innerClient.CreatePipeline()){
                            
                            //update the user's name and status
                            pipeline.QueueCommand(client => client.SetRangeInHash(RedisPaths.ModelServerInfo, new KeyValuePair<string,string>[]{
                                new KeyValuePair<string, string>("status", "connected"),
                                new KeyValuePair<string, string>("lastupdated", msg.Timestamp.ToString()),
                            }));

                            //publish events
                            pipeline.QueueCommand(client => client.PublishMessage(RedisPaths.ModelServerInfo, "changed"));

                            pipeline.Flush();
                        }

                        result = true;
                    }
                    else if(raw.StartsWith("{\"MessageType\":\"Disconnected\"")){
                        var msg = JsonSerializer.Deserialize<Disconnected>(raw);

                        using(var innerClient = redisPool.GetClient())
                        using(var pipeline = innerClient.CreatePipeline()){
                            
                            //update the user's name and status
                            pipeline.QueueCommand(client => client.SetRangeInHash(RedisPaths.ModelServerInfo, new KeyValuePair<string,string>[]{
                                new KeyValuePair<string, string>("status", "disconnected"),
                                new KeyValuePair<string, string>("lastupdated", msg.Timestamp.ToString()),
                                new KeyValuePair<string, string>("lastdisconnectreason", msg.ExtraInfo != null ? msg.ExtraInfo : "none" ),
                            }));

                            //publish events
                            pipeline.QueueCommand(client => client.PublishMessage(RedisPaths.ModelServerInfo, "changed"));
                            
                            pipeline.Flush();
                        }

                        result = true;
                    }
                }
                catch(Exception){
                    result = false;
                }  
            }

            return result;
        }

        ///Redis pubsub will call this when new control messages are available in the fresh list
        private void NewDataReady(string channel, string message){
            
            Console.WriteLine("Shredding");
            ShredFreshData();
            
        }

        
    }
}
