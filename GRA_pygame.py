import numpy
import math
import tkinter
import pygame
import os
import platform

pygame.init()
#all the point are 2D; using numpy.array
#all vector, configuration are 1D

#background_initialization------------------------------------------------------
scale = 400/128 # display / model
display_width = int(128*scale)
display_height = int(128*scale)
box_edge = numpy.array([[0,0], [0,127], [127,127], [127,0]])

#function_setup-----------------------------------------------------------------

#--convert planning vertices into display vertices (x, y) => (x*scale, H-y*scale)
#----matrix <- numpy.array (1D/2D)
def planning_to_display(matrix):
    return matrix.dot(numpy.matrix([[scale,0],[0,-scale]])) + numpy.array([0, display_height])

#--convert display vertices into planning vertices (x, y) => (x/scale, (H-y)/scale)
#----matrix <- numpy.matrix (1D/2D)
def display_to_planning(matrix):
    return (matrix - numpy.array([0, display_height])).dot(numpy.array([[1/scale,0],[0,-1/scale]]))

#--Translation and Rotation; matrix is 2D; xytheta = (dx, dy, theta in degrees)
#----matrix <- numpy.array (2D); xytheta <- list / numpy.array (1D)
def TR(matrix, xytheta):
    temp = numpy.ones(matrix.shape[0], dtype=int).reshape((matrix.shape[0],1))
    temp = numpy.concatenate((matrix, temp), axis=1)
    (dx, dy, theta) = (xytheta[0], xytheta[1], math.radians(xytheta[2]))
    temp = temp.dot(numpy.array([[math.cos(theta), math.sin(theta),0], [-math.sin(theta), math.cos(theta),0], [dx, dy, 1]]))
    return temp[:,:2]

#--angle_compute
#----vector1 <- list / numpy.array (1D); vector2 <- list / numpy.array (1D)
def get_angle(vector1, vector2):
    vector1 = vector1.reshape((2,))
    vector2 = vector2.reshape((2,))
    return math.degrees(math.atan2(vector2[1], vector2[0]) - math.atan2(vector1[1], vector1[0]))

#--assign the point in obstacles or not; point as matrix, edge as xytheta
#----vector_of_point <- list / numpy.array (1D); vector_of_edge <- list / numpy.array(1D)
def point_left(vector_of_point, vector_of_edge):
    temp = TR(numpy.array(vector_of_point).reshape((1,2)), [0,0,-math.degrees(math.atan2(vector_of_edge[1], vector_of_edge[0]))])
    return temp[0,1] >= 0

#objects_setup------------------------------------------------------------------

#--objects
#----conf = configuration (x, y, angle) <- list (1D)
#----vertices <- list (3D) and counterclockwise ordered
#----control = control point (x, y) <- list (2D)
class robots:
    def __init__(self, conf, n_polygon, vertices, n_control, control):
        self.n_polygon = n_polygon
        self.n_control = n_control
        self.world_conf = numpy.array(conf).astype(float) #<----flexible
        self.world_polygon = [numpy.array(vertices[i]) for i in range(self.n_polygon)] #<----fixed
        self.world_control = numpy.array(control) #<----fixed

        self.planning_conf = self.world_conf #<----flexible
        self.planning_polygon = [TR(self.world_polygon[i], self.planning_conf).astype(int) for i in range(self.n_polygon)]
        self.planning_control = TR(self.world_control, self.planning_conf).astype(int)
        
        self.planning_bounding_box = numpy.array(self.planning_polygon).flatten().astype(int)
        self.planning_bounding_box = self.planning_bounding_box.reshape((int(self.planning_bounding_box.size/2), 2))
        self.planning_bounding_box = numpy.array([self.planning_bounding_box.min(0), self.planning_bounding_box.max(0)])

        self.display_polygon = [planning_to_display(polygon) for polygon in self.planning_polygon]
        
        self.display_bounding_box = numpy.array(self.display_polygon).flatten().astype(int)
        self.display_bounding_box = self.display_bounding_box.reshape((int(self.display_bounding_box.size/2), 2))
        self.display_bounding_box = numpy.array([self.display_bounding_box.min(0), self.display_bounding_box.max(0)])

    def mouse_inside(self, mouse_position): #mouse_position <- list (1D)
        if mouse_position[0] in range(self.display_bounding_box[0,0], self.display_bounding_box[1,0])\
        and mouse_position[1] in range(self.display_bounding_box[0,1], self.display_bounding_box[1,1]):
            return True
        else:
            return False

    def display_change(self, position1, position2, mouse_left=True): #position1,2 <- numpy.array (1D) on the display map
        if mouse_left == True: #move<----(dx, dy)
            move = display_to_planning(position2) - display_to_planning(position1)
            self.planning_conf[:2] += move

        else: #move<----theta
            position1 = (display_to_planning(position1) - self.planning_conf[:2])
            position2 = (display_to_planning(position2) - self.planning_conf[:2])
            move = get_angle(position1, position2)
            self.planning_conf[-1] += move
            self.planning_conf[-1] %= 360

        self.planning_polygon = [TR(self.world_polygon[i], self.planning_conf).astype(int) for i in range(self.n_polygon)]
        self.planning_control = TR(self.world_control, self.planning_conf).astype(int)
        self.planning_bounding_box = numpy.array(self.planning_polygon).flatten().astype(int)
        self.planning_bounding_box = self.planning_bounding_box.reshape((int(self.planning_bounding_box.size/2), 2))
        self.planning_bounding_box = numpy.array([self.planning_bounding_box.min(0), self.planning_bounding_box.max(0)])

        self.display_polygon = [planning_to_display(polygon) for polygon in self.planning_polygon]
        self.display_bounding_box = numpy.array(self.display_polygon).flatten().astype(int)
        self.display_bounding_box = self.display_bounding_box.reshape((int(self.display_bounding_box.size/2), 2))
        self.display_bounding_box = numpy.array([self.display_bounding_box.min(0), self.display_bounding_box.max(0)])

    def display_draw(self, game, color, width=0):
        for i in range(self.n_polygon):
            pygame.draw.polygon(game, color, numpy.array(self.display_polygon[i]), width)

    def NF1(self):
        U = {0: numpy.ones(128*128).reshape(128,128) * 255 } #initial potential = 255
        for obstacle in display_objects[2:]: #obstacle potential = 260
            for x in range(obstacle.planning_bounding_box[0,0], obstacle.planning_bounding_box[1,0]+1):
                for y in range(obstacle.planning_bounding_box[0,1], obstacle.planning_bounding_box[1,1]+1):
                    if obstacle.point_inside([x,y]):
                        U[0][127-y,x] = 260

        for n in range(self.n_control):
            U[n+1] = U[0].copy()
            U[n+1][128-self.planning_control[n][1], self.planning_control[n][0]] = 0
            L = {0: [numpy.array([0,0,0])], 1: []} #(dx,dy, delta theta) based on self.planning_control[n,:]
            order = 0
            while L[0]: # L[0] is not empty --> return True
                L[1] = []
                for q in L[0]:
                    control = TR(self.world_control[n].reshape((1,2)), (self.planning_conf+q)).reshape((2,)).astype(int) #current control point (x,y) <- numpy.array (1D)
                    for dx in (1,-1):
                        if (0<= (control[0]+dx) <=127): #bounded
                            if (U[n+1][127-control[1], control[0]+dx] == 255):
                                U[n+1][127-control[1], control[0]+dx] = order + 1
                                L[1] += [(q+(dx,0,0)).astype(int)]
                    for dy in (1,-1):
                        if (0<= (control[1]+dy) <=127): #bounded
                            if (U[n+1][127-control[1]-dy, control[0]] == 255):
                                U[n+1][127-control[1]-dy, control[0]] = order + 1
                                L[1] += [(q+(0,dy,0)).astype(int)]
                    for theta in (1,-1):
                        control = TR(self.world_control[n].reshape((1,2)), (self.planning_conf+q+(0,0,theta))).reshape((2,)).astype(int)
                        if (0,0)<= tuple(control) <=(127,127):
                            if (U[n+1][127-control[1], control[0]] == 255):
                                U[n+1][127-control[1], control[0]] = order + 1
                                L[1] += [(q+(0,0,theta)).astype(int)]
                L[0] = L[1]
                order += 1
        U_total = U[1]
        for n in range(self.n_control-1):
            U_total = numpy.maximum(U_total, U[2+n])
        print(U_total)

class obstacles:
    def __init__(self, conf, n_polygon, vertices):
        self.n_polygon = n_polygon
        self.world_conf = numpy.array(conf).astype(float) #<----flexible
        self.world_polygon = [numpy.array(vertices[i]) for i in range(self.n_polygon)] #<----fixed

        self.planning_conf = self.world_conf
        self.planning_polygon = [TR(self.world_polygon[i], self.planning_conf).astype(int) for i in range(self.n_polygon)]
        
        self.planning_bounding_box = numpy.array(self.planning_polygon).flatten().astype(int)
        self.planning_bounding_box = self.planning_bounding_box.reshape((int(self.planning_bounding_box.size/2), 2))
        self.planning_bounding_box = numpy.array([self.planning_bounding_box.min(0), self.planning_bounding_box.max(0)])

        self.display_polygon = [planning_to_display(polygon) for polygon in self.planning_polygon]
        
        self.display_bounding_box = numpy.array(self.display_polygon).flatten().astype(int)
        self.display_bounding_box = self.display_bounding_box.reshape((int(self.display_bounding_box.size/2), 2))
        self.display_bounding_box = numpy.array([self.display_bounding_box.min(0), self.display_bounding_box.max(0)])

    def mouse_inside(self, mouse_position): #mouse_position <- list (1D)
        if mouse_position[0] in range(self.display_bounding_box[0,0], self.display_bounding_box[1,0])\
        and mouse_position[1] in range(self.display_bounding_box[0,1], self.display_bounding_box[1,1]):
            return True
        else:
            return False

    def point_inside(self, point): #point <- list (1D)
        in_or_not = [True] * self.n_polygon
        for i in range(self.n_polygon):
            vector = (-1) * numpy.identity(self.planning_polygon[i].shape[0]) + numpy.eye(self.planning_polygon[i].shape[0], k=1)
            vector[-1,0] = 1
            vector = vector.dot(self.planning_polygon[i])
            points = numpy.full_like(self.planning_polygon[i], point) - self.planning_polygon[i]
            for j in range(self.planning_polygon[i].shape[0]):
                in_or_not[i] = in_or_not[i] & point_left(points[j,:], vector[j,:])
        return sum(in_or_not) > 0

    def display_change(self, position1, position2, mouse_left=True): #position1,2 <- numpy.array (1D) on the display map
        if mouse_left == True: #move<----(dx, dy)
            move = display_to_planning(position2) - display_to_planning(position1)
            self.planning_conf[:2] += move

        else: #move<----theta
            position1 = (display_to_planning(position1) - self.planning_conf[:2])
            position2 = (display_to_planning(position2) - self.planning_conf[:2])
            move = get_angle(position1, position2)
            self.planning_conf[-1] += move
            self.planning_conf[-1] %= 360

        self.planning_polygon = [TR(self.world_polygon[i], self.planning_conf).astype(int) for i in range(self.n_polygon)]
        self.planning_bounding_box = numpy.array(self.planning_polygon).flatten().astype(int)
        self.planning_bounding_box = self.planning_bounding_box.reshape((int(self.planning_bounding_box.size/2), 2))
        self.planning_bounding_box = numpy.array([self.planning_bounding_box.min(0), self.planning_bounding_box.max(0)])

        self.display_polygon = [planning_to_display(polygon) for polygon in self.planning_polygon]
        self.display_bounding_box = numpy.array(self.display_polygon).flatten().astype(int)
        self.display_bounding_box = self.display_bounding_box.reshape((int(self.display_bounding_box.size/2), 2))
        self.display_bounding_box = numpy.array([self.display_bounding_box.min(0), self.display_bounding_box.max(0)])

    def display_draw(self, game, color, width=0):
        for i in range(self.n_polygon):
            pygame.draw.polygon(game, color, numpy.array(self.display_polygon[i]), width)

#data_input--------------------------------------------------------------------

n_robots = 2
n_obstacles = 3

robots0_recent = robots(conf = [64, 64, 90], n_polygon = 2, \
                vertices = [[[15,4], [-3,4], [-3,-4], [15,-4]], [[7,4], [11,4], [11,8], [7,8]]], \
                n_control = 2, control = [[12,10], [-2,0]])

robots0_goal = robots(conf = [80,80,0], n_polygon = 2, \
                vertices = [[[15,4], [-3,4], [-3,-4], [15,-4]], [[7,4], [11,4], [11,8], [7,8]]], \
                n_control = 2, control = [[12,10], [-2,0]])

robots1_recent = robots(conf = [20, 20, 90], n_polygon = 1, \
                vertices = [[[-5,-5], [5,-5], [0,5]]], \
                n_control = 2, control = [[0,-4], [0,4]])

robots1_goal = robots(conf = [30,100,0], n_polygon = 1, \
                vertices = [[[-5,-5], [5,-5], [0,5]]], \
                n_control = 2, control = [[0,-4], [0,4]])

obstacles0 = obstacles(conf = [40, 30, 300], n_polygon = 1, \
            vertices = [[[9,-7], [13,0], [9,6], [-11,6], [-14,0], [-11,-7]]])

obstacles1 = obstacles(conf = [90, 51, 3.75], n_polygon = 1, \
            vertices = [[[17,6], [-17,6], [-17,-7], [25,-7]]])

obstacles2 = obstacles(conf = [56,30,90], n_polygon = 2, \
            vertices = [[[9,-3], [9,6], [-11,6], [-11,-3]],[[1,6], [1,10], [-2,10], [-2,6]]])

display_objects = [robots0_recent, robots0_goal, obstacles0, obstacles1, obstacles2]


#tkinter_initialization--------------------------------------------------------
root = tkinter.Tk()
root.title("Motion Planning - Control Board")
pygame_win = tkinter.Frame(root, width = 200, height = 200, background="gray14")
pygame_win.pack(fill=tkinter.X, padx=100, pady=100)

##Label, Checkbotton, Botton
check = tkinter.BooleanVar()
Check_lock = tkinter.Checkbutton(pygame_win, text="Lock", variable=check, width=10)

option_var = tkinter.StringVar(pygame_win)
option_var.set("robot #0")
Option = tkinter.OptionMenu(pygame_win, option_var, "robot #0", "robot #1")

Button_NF1 = tkinter.Button(pygame_win, text="Show NF1", command = display_objects[1].NF1)

Check_lock.pack(fill=tkinter.Y, side=tkinter.LEFT, expand=1)
Option.pack(fill=tkinter.BOTH, expand=1)
Button_NF1.pack(fill=tkinter.BOTH, expand=1)

##embed pygame into tkinter
os.environ['SDL_WINDOWID'] = str(pygame_win.winfo_id())
if platform.system() == 'Darwin': # <- for Mac OS
    os.environ['SDL_VIDEODRIVER'] = 'Quartz' 
elif platform.system() == 'Windows': # <- for Windows
    os.environ['SDL_VIDEODRIVER'] = 'windib'

root.update()

#pygame_main-------------------------------------------------------------------
black = (0,0,0)
white = (255,255,255)
red = (255,0,0)
blue = (0,0,139)

gameDisplay = pygame.display.set_mode((display_width,display_height))
pygame.display.set_caption('Motion Planning - Displaying board')
clock = pygame.time.Clock()

def main():

    while True:
        event = pygame.event.poll()
        if event.type == pygame.QUIT:
            pygame.quit()
            root.quit()
            quit()
        
        if option_var.get() == "robot #0":
            display_objects = [robots0_recent, robots0_goal, obstacles0, obstacles1, obstacles2]
            Button_NF1.config(command = robots0_goal.NF1)
        else:
            display_objects = [robots1_recent, robots1_goal, obstacles0, obstacles1, obstacles2]
            Button_NF1.config(command = robots1_goal.NF1)

#mouse_control-----------------------------------------------------------------
        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1: #mouse_left
            mouse_position1 = numpy.array(pygame.mouse.get_pos())
            
            while event.type != pygame.MOUSEBUTTONUP:
                event = pygame.event.poll()
                mouse_position2 = pygame.mouse.get_pos()

            mouse_position2 = numpy.array(mouse_position2)
            
            for object in display_objects:
                if object.mouse_inside(mouse_position1):
                    object.display_change(mouse_position1, mouse_position2, mouse_left=True)
                else:
                    pass

        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 3: #mouse_right
            mouse_position1 = numpy.array(pygame.mouse.get_pos())

            while event.type != pygame.MOUSEBUTTONUP:
                event = pygame.event.poll()
                mouse_position2 = pygame.mouse.get_pos()

            mouse_position2 = numpy.array(mouse_position2)
            
            for object in display_objects:
                if object.mouse_inside(mouse_position1):
                    object.display_change(mouse_position1, mouse_position2, mouse_left=False)
                else:
                    pass

#model section-----------------------------------------------------------------
        
#display section---------------------------------------------------------------
        gameDisplay.fill(white)

        pygame.draw.polygon(gameDisplay, blue, numpy.array(planning_to_display(box_edge)), 1)

        display_objects[0].display_draw(gameDisplay, black) #draw robot_recent
        display_objects[1].display_draw(gameDisplay, black, width=2) #draw robot_goal

        for object in display_objects[2:]: #draw obstacle
            object.display_draw(gameDisplay, red)

        pygame.display.update()
        root.update()
        clock.tick(30)

main()
