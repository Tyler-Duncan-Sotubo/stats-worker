import { IsOptional, IsString } from 'class-validator';

export class BackfillRangeDto {
  @IsString()
  fromDate!: string;

  @IsString()
  @IsOptional()
  toDate!: string;
}
