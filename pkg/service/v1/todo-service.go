package v1

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"

	v1 "github.com/AndreyLM/go-grpc-rest/pkg/api/v1"
	context "golang.org/x/net/context"
	"google.golang.org/grpc/codes"

	"google.golang.org/grpc/status"
)

const (
	apiVersion = "v1"
)

// ToDoServer - inplementation of v1.ToDoServiceServer proto interface
type ToDoServer struct {
	db *sql.DB
}

// NewToDoServiceServer - new server
func NewToDoServiceServer(db *sql.DB) v1.ToDoServiceServer {
	return &ToDoServer{db: db}
}

func (s *ToDoServer) checkAPI(api string) error {
	if len(api) > 0 {
		if apiVersion != api {
			return status.Errorf(
				codes.Unimplemented,
				"unsupported API version: service implements API verion")
		}
	}

	return nil
}

func (s *ToDoServer) connect(ctx context.Context) (*sql.Conn, error) {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to connect to databse"+err.Error())
	}

	return c, nil
}

// Create - creates new todo item
func (s *ToDoServer) Create(ctx context.Context, req *v1.CreateRequest) (*v1.CreateResponse, error) {
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	reminder, err := ptypes.Timestamp(req.ToDo.Reminder)
	if err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"reminder field has invalid format->"+err.Error(),
		)
	}
	res, err := c.ExecContext(
		ctx,
		"INSERT INTO ToDo(`Title`, `Description`, `Reminder`) VALUES(?, ?, ?)",
		req.ToDo.Title, req.ToDo.Description, reminder,
	)

	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to insert into ToDo->"+err.Error())
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed retrieve id for created ToDo->"+err.Error())
	}

	return &v1.CreateResponse{
		Api: apiVersion,
		Id:  id,
	}, nil
}

// Read - read ToDo by Id
func (s *ToDoServer) Read(ctx context.Context, req *v1.ReadRequest) (*v1.ReadResponse, error) {
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	rows, err := c.QueryContext(ctx, "SELECT `ID`, `Title`, `Description`, `Reminder`"+
		" FROM ToDo WHERE `ID`=?", req.Id)

	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select from ToDo->"+err.Error())
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, status.Error(codes.Unknown, "failed to retrieve data from ToDo->"+err.Error())
		}
		return nil, status.Error(codes.NotFound, fmt.Sprintf(
			"ToDo with ID='%d' is not found", req.Id))
	}

	var td v1.ToDo
	var reminder time.Time
	if err := rows.Scan(&td.Id, &td.Title, &td.Description, &reminder); err != nil {
		return nil, status.Error(
			codes.Unknown,
			"failed to retrive field values from ToDo row -> "+err.Error())

	}
	td.Reminder, err = ptypes.TimestampProto(reminder)
	if err != nil {
		return nil, status.Error(codes.Unknown, "remider filed has invalid format->"+err.Error())
	}

	if rows.Next() {
		return nil, status.Error(
			codes.Unknown,
			fmt.Sprintf("found multiple ToDo rows for id->%d", req.Id),
		)
	}

	return &v1.ReadResponse{
		Api:  apiVersion,
		ToDo: &td,
	}, nil
}

// Update - updates ToDo item
func (s *ToDoServer) Update(ctx context.Context, req *v1.UpdateRequest) (*v1.UpdateResponse, error) {
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	reminder, err := ptypes.Timestamp(req.ToDo.Reminder)

	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "reminder field is invalid")
	}

	res, err := c.ExecContext(ctx, "UPDATE ToDo SET `Title`=?, `Description`=?, `Reminder`=? WHERE `ID`=?",
		req.ToDo.Title, req.ToDo.Description, reminder, req.ToDo.Id)

	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to update ToDo->"+err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, status.Error(codes.Unknown, "fail to retrieve updated rows")
	}

	if rows == 0 {
		return nil, status.Error(codes.NotFound, "Todo not found")
	}

	return &v1.UpdateResponse{
		Api:     apiVersion,
		Updated: rows,
	}, nil
}

// Delete - deletes ToDo item from list
func (s *ToDoServer) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	res, err := c.ExecContext(ctx, "DELETE FROM ToDo WHERE `ID`=?", req.Id)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to delete ToDo->"+err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to retrieve affected rows")
	}

	if rows == 0 {
		return nil, status.Error(
			codes.NotFound,
			fmt.Sprintf("ToDo with id->%d not found", req.Id),
		)
	}

	return &v1.DeleteResponse{
		Api:     apiVersion,
		Deleted: rows,
	}, nil
}

// ReadAll - reads all ToDo list
func (s *ToDoServer) ReadAll(ctx context.Context, req *v1.ReadAllRequest) (*v1.ReadAllResponse, error) {
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	rows, err := c.QueryContext(ctx, "SELECT * FROM ToDo")
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to get ToDo list->"+err.Error())
	}
	defer rows.Close()

	var reminder time.Time
	list := []*v1.ToDo{}
	for rows.Next() {
		td := new(v1.ToDo)
		if err := rows.Scan(&td.Id, &td.Title, &td.Description, &reminder); err != nil {
			return nil, status.Error(codes.Unknown, "failed to retrieve ToDo row->"+err.Error())
		}

		td.Reminder, err = ptypes.TimestampProto(reminder)
		if err != nil {
			return nil, status.Error(codes.Unknown, "reminder has invalid format -> "+err.Error())
		}
		list = append(list, td)
	}

	if err := rows.Err(); err != nil {
		return nil, status.Error(codes.Unknown, "failed to retrieve data from ToDo->"+err.Error())
	}

	return &v1.ReadAllResponse{
		Api:   apiVersion,
		ToDos: list,
	}, nil
}
