/* eslint-disable @typescript-eslint/no-unsafe-argument */
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { AppModule } from './app.module';
import { Logger } from 'nestjs-pino';
import { ValidationPipe } from '@nestjs/common';
import fastifyCompress from '@fastify/compress';
import fastifyCookie from '@fastify/cookie';
import multipart from '@fastify/multipart';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({ logger: false, bodyLimit: 10 * 1024 * 1024 }),
    {
      bufferLogs: true,
      bodyParser: false,
    },
  );

  app.useLogger(app.get(Logger));
  app.setGlobalPrefix('api');

  const fastify = app.getHttpAdapter().getInstance();

  await fastify.register(multipart as any, {
    limits: { fileSize: 10 * 1024 * 1024 },
  });

  await fastify.register(fastifyCompress as any);

  await fastify.register(fastifyCookie as any, {
    secret: process.env.COOKIE_SECRET,
  });

  app.enableCors({
    origin: [process.env.CLIENT_URL].filter(
      (url): url is string => typeof url === 'string',
    ),
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
    credentials: true,
  });

  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  const port = process.env.PORT || 8001;
  await app.listen(port, '0.0.0.0');
  app.get(Logger).log(`🚀 Listening on port ${port}`);
}

void bootstrap();
