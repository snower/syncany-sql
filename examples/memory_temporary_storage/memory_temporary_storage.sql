
insert into temporary_a select Id, Email, FirstName, LastName from `data/user.json`;

insert into temporary_b (Id, Name) VALUES ('aa7e941b-d399-4bec-0ba4-08d8dd2f9239', 'LiMei');

select * from temporary_a;

select * from temporary_b;

select a.Id, a.Email, b.Name, a.FirstName, a.LastName from temporary_a a join temporary_b b on a.Id=b.Id;