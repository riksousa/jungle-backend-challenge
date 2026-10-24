import 'reflect-metadata';
import { DataSource, QueryRunner } from 'typeorm';
import { Initial1770000000000 } from './migrations/initial';

export const dataSource = new DataSource({
  type: 'postgres',
  url: process.env.DATABASE_URL ?? 'postgres://jungle:jungle@localhost:5432/jungle',
  migrations: [Initial1770000000000],
  migrationsRun: false,
  synchronize: false,
  logging: false,
  extra: { max: 20, connectionTimeoutMillis: 5000, statement_timeout: 15000 },
});

export async function transaction<T>(work: (runner: QueryRunner) => Promise<T>): Promise<T> {
  const runner = dataSource.createQueryRunner();
  await runner.connect();
  await runner.startTransaction('READ COMMITTED');
  try {
    const result = await work(runner);
    await runner.commitTransaction();
    return result;
  } catch (error) {
    await runner.rollbackTransaction();
    throw error;
  } finally {
    await runner.release();
  }
}

export async function connectDatabase(): Promise<void> {
  if (!dataSource.isInitialized) await dataSource.initialize();
  const runner=dataSource.createQueryRunner();
  await runner.connect();
  try {
    await runner.query('SELECT pg_advisory_lock(748291036)');
    await dataSource.runMigrations({transaction:'all'});
  } finally {
    await runner.query('SELECT pg_advisory_unlock(748291036)');
    await runner.release();
  }
}
export async function closeDatabase(): Promise<void> {
  if (dataSource.isInitialized) await dataSource.destroy();
}
