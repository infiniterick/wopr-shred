using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using MassTransit;
using ServiceStack.Redis;
using Wopr.Core;

namespace Wopr.Shred
{
    public class Shredder
    {
        RedisManagerPool redisPool;
        CancellationToken cancel;
        IBusControl bus;
        string rabbitToken;
        bool mock = false;

        public Shredder(Secrets secrets, CancellationToken cancel) {
            redisPool = new RedisManagerPool(secrets.RedisToken);
            this.cancel = cancel;
            this.rabbitToken = secrets.RabbitToken;
        }

        public void Start(){
            StartMassTransit().Wait();
        }

        public void Stop(){
        }

        private Task StartMassTransit(){
            bus = Bus.Factory.CreateUsingRabbitMq(sbc => {
                var parts = rabbitToken.Split('@');
                sbc.Host(new Uri(parts[2]), cfg => {
                    cfg.Username(parts[0]);
                    cfg.Password(parts[1]);
                });
                rabbitToken = string.Empty;

                sbc.ReceiveEndpoint("wopr:discord:new", ep =>
                {
                    
                    if(mock){
                        ep.Consumer<UserUpdatedConsumerMock>(()=>{return new UserUpdatedConsumerMock(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ContentReceivedConsumerMock>(()=>{return new ContentReceivedConsumerMock(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ReactionAddedConsumerMock>(()=>{return new ReactionAddedConsumerMock(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ReactionRemovedConsumerMock>(()=>{return new ReactionRemovedConsumerMock(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ConnectedConsumerMock>(()=>{return new ConnectedConsumerMock(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<DisconnectedConsumerMock>(()=>{return new DisconnectedConsumerMock(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                    }else{
                        ep.Consumer<UserUpdatedConsumer>(()=>{return new UserUpdatedConsumer(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ContentReceivedConsumer>(()=>{return new ContentReceivedConsumer(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ReactionAddedConsumer>(()=>{return new ReactionAddedConsumer(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ReactionRemovedConsumer>(()=>{return new ReactionRemovedConsumer(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<ConnectedConsumer>(()=>{return new ConnectedConsumer(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                        ep.Consumer<DisconnectedConsumer>(()=>{return new DisconnectedConsumer(redisPool);}, cc => cc.UseConcurrentMessageLimit(1));
                    }
                    
                });
                
            });

            return bus.StartAsync(); // This is important!-
        }
    }
}
