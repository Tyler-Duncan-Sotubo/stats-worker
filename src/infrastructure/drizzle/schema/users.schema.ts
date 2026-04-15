import { pgTable, text, timestamp, uniqueIndex } from 'drizzle-orm/pg-core';
import { defaultId } from '../default-id';
import { roleEnum } from './enum';

export const users = pgTable(
  'users',
  {
    id: text('id').primaryKey().$defaultFn(defaultId),
    name: text('name').notNull(),
    email: text('email').notNull(),
    password: text('password').notNull(),
    role: roleEnum('role').default('contributor').notNull(),
    location: text('location'),
    avatar: text('avatar'),
    createdAt: timestamp('created_at').defaultNow().notNull(),
    updatedAt: timestamp('updated_at').defaultNow().notNull(),
  },
  (t) => [uniqueIndex('users_email_idx').on(t.email)],
);
